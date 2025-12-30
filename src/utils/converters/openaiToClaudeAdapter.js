/**
 * OpenAI → Claude 请求转换适配器
 *
 * 职责：
 * 1. 请求转换：OpenAI Chat Completions API → Claude Messages API
 * 2. 响应转换：OpenAI 流式响应 → Claude SSE 格式
 * 3. 工具调用格式转换：OpenAI tools → Claude tools
 *
 * 支持的 OpenAI 内容类型：
 * - text: 纯文本
 * - image_url (base64/url): 图片
 * - file: 文件/文档
 * - tool_calls: 工具调用
 * - tool role: 工具结果
 */

import { generateRequestId, generateToolUseId } from '../idGenerator.js';
import { ToolConverter } from './common/toolConverter.js';
import { resolveThinkingBudget } from './thinkingConfig.js';
import { mapOpenAIFinishToClaude } from './stopReasonMapper.js';
import { writeSSE, buildMessageStartPayload } from './sseUtils.js';
import { estimateTokensFromText } from './tokenUtils.js';

import { normalizeMessagesForClaude } from './messageUtils.js';
import { safeJsonParse, safeJsonStringify } from '../utils.js';

// ==================== 【请求转换】OpenAI → Claude ====================

/**
 * 【请求转换】将 OpenAI image_url 转换为 Claude image source
 * @param {object} imageUrl - OpenAI image_url 对象
 * @returns {object|null} - Claude image block 或 null
 */
function convertOpenAIImageToClaude(imageUrl) {
  const url = imageUrl?.url;
  if (!url) return null;

  // 检查是否为 base64 Data URL
  const base64Match = url.match(/^data:([^;]+);base64,(.+)$/);
  if (base64Match) {
    return {
      type: 'image',
      source: {
        type: 'base64',
        media_type: base64Match[1],
        data: base64Match[2]
      }
    };
  }

  // 普通 URL
  if (url.startsWith('http://') || url.startsWith('https://')) {
    return {
      type: 'image',
      source: {
        type: 'url',
        url: url
      }
    };
  }

  return null;
}

/**
 * 将 OpenAI input_file 转换为 Claude document source
 * @param {object} filePart - OpenAI input_file 对象
 * @returns {object|null} - Claude document block 或 null
 */
function convertOpenAIFileToClaude(filePart) {
  let source = null;

  // 1. filename + file_data (Data URL)
  if (filePart.file_data) {
    const base64Match = filePart.file_data.match(/^data:([^;]+);base64,(.+)$/);
    if (base64Match) {
      source = {
        type: 'base64',
        media_type: base64Match[1],
        data: base64Match[2]
      };
    }
  }
  // 2. file_id 引用
  else if (filePart.file_id) {
    source = {
      type: 'file',
      file_id: filePart.file_id
    };
  }

  if (!source) return null;

  // 3. 组装 Document Block
  const documentBlock = {
    type: 'document',
    source
  };

  // 映射 filename/title -> title (优先使用 explicit title)
  const docTitle = filePart.title || filePart.filename;
  if (docTitle) {
    documentBlock.title = docTitle;
  }

  // 透传 context (如果有)
  if (filePart.context) {
    documentBlock.context = filePart.context;
  }

  return documentBlock;
}

/**
 * 将 OpenAI 内容转换为 Claude 内容块数组
 * @param {string|Array} content - OpenAI 消息内容
 * @returns {Array} - Claude 内容块数组
 */
function convertOpenAIContentToClaude(content) {
  // 字符串内容
  if (typeof content === 'string') {
    return [{ type: 'text', text: content }];
  }

  if (!Array.isArray(content)) {
    return [{ type: 'text', text: '' }];
  }

  const blocks = [];

  for (const part of content) {
    if (!part || typeof part !== 'object') continue;

    switch (part.type) {
      case 'text':
        if (part.text) {
          blocks.push({ type: 'text', text: part.text });
        }
        break;

      case 'image_url':
        const imageBlock = convertOpenAIImageToClaude(part.image_url);
        if (imageBlock) {
          blocks.push(imageBlock);
        }
        break;

      case 'file':
        // OpenAI Chat Completions API 文件格式
        const fileBlock = convertOpenAIFileToClaude(part.file);
        if (fileBlock) {
          blocks.push(fileBlock);
        }
        break;

      // case 'input_text':
      //   // OpenAI Responses API 格式
      //   if (part.text) {
      //     blocks.push({ type: 'text', text: part.text });
      //   }
      //   break;

      // case 'input_image':
      //   // OpenAI Responses API 格式
      //   const respImageBlock = convertOpenAIImageToClaude({ url: part.image_url });
      //   if (respImageBlock) {
      //     blocks.push(respImageBlock);
      //   }
      //   break;

      // case 'input_file':
      //   // OpenAI Responses API 文档格式
      //   const inputFileBlock = convertOpenAIFileToClaude(part);
      //   if (inputFileBlock) {
      //     blocks.push(inputFileBlock);
      //   }
      //   break;

      // case 'reasoning':
      //   // OpenAI Responses API reasoning 格式转换为 Claude thinking
      //   const thinkingText = Array.isArray(part.summary)
      //     ? part.summary.map(s => s.text || '').join('')
      //     : '';

      //   if (thinkingText) {
      //     const thinkingBlock = {
      //       type: 'thinking',
      //       thinking: thinkingText
      //     };
      //     // 如果有 signature 则透传
      //     if (part.signature) {
      //       thinkingBlock.signature = part.signature;
      //     }
      //     blocks.push(thinkingBlock);
      //   }
      //   break;
    }
  }

  return blocks.length > 0 ? blocks : [{ type: 'text', text: '' }];
}

/**
 * 将 OpenAI tool_calls 转换为 Claude tool_use 块
 * @param {Array} toolCalls - OpenAI tool_calls 数组
 * @returns {Array} - Claude tool_use 块数组
 */
function convertOpenAIToolCallsToClaude(toolCalls) {
  if (!Array.isArray(toolCalls)) return [];

  return toolCalls.map(tc => {
    const args = safeJsonParse(tc?.function?.arguments, {});
    return {
      type: 'tool_use',
      id: tc.id || generateToolUseId(),
      name: tc.function?.name || 'unknown',
      input: args
    };
  });
}

/**
 * 将 OpenAI tool 消息转换为 Claude tool_result 块
 * 支持多模态内容（文本、图片、文档）
 * @param {object} message - OpenAI tool 消息
 * @returns {object} - Claude tool_result 块
 */
function convertOpenAIToolResultToClaude(message) {
  const rawContent = message.content;
  let isError = false;

  // 1. 错误检测
  if (typeof rawContent === 'string') {
    isError = /^error:\s*/i.test(rawContent);
  } else if (Array.isArray(rawContent) && rawContent.length > 0) {
    // 在多模态内容中查找第一个文本块检测错误
    const firstText = rawContent.find(b => b.type === 'text');
    if (firstText && typeof firstText.text === 'string') {
      isError = /^error:\s*/i.test(firstText.text);
    }
  }

  // 2. 转换内容 (支持文本、图片、文档等)
  const content = convertOpenAIContentToClaude(rawContent);

  const result = {
    type: 'tool_result',
    tool_use_id: message.tool_call_id,
    content: content
  };

  // 3. 添加错误标记
  if (isError) {
    result.is_error = true;
  }

  return result;
}

/**
 * 将 OpenAI 消息数组转换为 Claude 消息数组
 * @param {Array} messages - OpenAI 消息数组
 * @returns {Array} - Claude 消息数组
 */
function convertOpenAIMessagesToClaude(messages) {
  const claudeMessages = [];

  for (const msg of messages) {
    if (msg.role === 'system') {
      // system 消息由 normalizeMessagesForClaude 处理
      continue;
    }

    if (msg.role === 'user') {
      claudeMessages.push({
        role: 'user',
        content: convertOpenAIContentToClaude(msg.content)
      });
    } else if (msg.role === 'assistant') {
      const content = [];

      // 添加文本内容
      if (msg.content) {
        content.push(...convertOpenAIContentToClaude(msg.content));
      }

      // 添加工具调用
      if (msg.tool_calls && msg.tool_calls.length > 0) {
        content.push(...convertOpenAIToolCallsToClaude(msg.tool_calls));
      }

      if (content.length > 0) {
        claudeMessages.push({
          role: 'assistant',
          content
        });
      }
    } else if (msg.role === 'tool') {
      // tool 消息作为 user 角色的 tool_result
      const lastMsg = claudeMessages[claudeMessages.length - 1];
      const toolResult = convertOpenAIToolResultToClaude(msg);

      if (lastMsg && lastMsg.role === 'user') {
        // 合并到上一条 user 消息
        if (!Array.isArray(lastMsg.content)) {
          lastMsg.content = [{ type: 'text', text: lastMsg.content || '' }];
        }
        lastMsg.content.push(toolResult);
      } else {
        // 创建新的 user 消息
        claudeMessages.push({
          role: 'user',
          content: [toolResult]
        });
      }
    }
  }

  return claudeMessages;
}

/**
 * 将 OpenAI tool_choice 转换为 Claude 格式
 * OpenAI: "auto" | "none" | "required" | {type: "function", function: {name: "xxx"}}
 * Claude: {type: "auto"/"any"/"tool"/"none", name?: string}
 */
function mapOpenAIToolChoiceToClaude(toolChoice) {
  if (!toolChoice) {
    return { type: 'auto' };
  }

  // 字符串格式
  if (typeof toolChoice === 'string') {
    switch (toolChoice) {
      case 'auto':
        return { type: 'auto' };
      case 'none':
        return { type: 'none' };
      case 'required':
        return { type: 'any' };
      default:
        return { type: 'auto' };
    }
  }

  // 对象格式：指定特定函数
  if (toolChoice.type === 'function' && toolChoice.function?.name) {
    return {
      type: 'tool',
      name: toolChoice.function.name
    };
  }

  return { type: 'auto' };
}

/**
 * 将 OpenAI 工具定义转换为 Claude 格式
 * @param {Array} tools - OpenAI tools 数组
 * @returns {Array} - Claude tools 数组
 */
function convertOpenAIToolsToClaude(tools) {
  return ToolConverter.toClaude(tools);
}

/**
 * 【请求转换 · 主入口】将 OpenAI Chat Completions API 请求体转换为 Claude Messages API 格式
 *
 * 转换方向: OpenAI → Claude
 *
 * @param {object} body - OpenAI 请求体
 * @returns {object} - Claude 请求体
 */
export function mapOpenAIToClaude(body) {
  if (!body || typeof body !== 'object') {
    throw new Error('请求体格式不合法');
  }
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    throw new Error('messages 不能为空');
  }

  // 1. 规范化消息（提取 system，合并连续角色，确保 user 开头）
  const { system, messages: normalizedMessages } = normalizeMessagesForClaude(body.messages);

  // 2. 转换消息内容
  const claudeMessages = convertOpenAIMessagesToClaude(normalizedMessages);

  // 3. 再次确保 user/assistant 交替
  const finalMessages = ensureAlternatingRoles(claudeMessages);

  const result = {
    model: body.model,
    max_tokens: body.max_tokens || 10000,
    messages: finalMessages,
    stream: body.stream !== false
  };

  // 添加 system
  if (system) {
    result.system = system;
  }

  // 添加工具
  if (body.tools && body.tools.length > 0) {
    result.tools = convertOpenAIToolsToClaude(body.tools);
    result.tool_choice = mapOpenAIToolChoiceToClaude(body.tool_choice);
  }

  // 添加可选参数
  if (body.temperature !== undefined) {
    result.temperature = body.temperature;
  }
  if (body.top_p !== undefined) {
    result.top_p = body.top_p;
  }

  // 处理 reasoning effort -> thinking budget
  if (body.reasoning && typeof body.reasoning === 'object') {
    const budgetTokens = resolveThinkingBudget(body.reasoning.effort);

    result.thinking = {
      type: 'enabled',
      budget_tokens: budgetTokens
    };

    // 确保 max_tokens 大于 budget_tokens (Claude API 硬性要求)
    if (result.max_tokens <= budgetTokens) {
      result.max_tokens = budgetTokens + 10000;
    }
  }

  return result;
}

/**
 * 确保消息交替出现
 */
function ensureAlternatingRoles(messages) {
  if (!messages || messages.length === 0) {
    return [{ role: 'user', content: [{ type: 'text', text: '' }] }];
  }

  const result = [];
  let lastRole = null;

  for (const msg of messages) {
    if (msg.role === lastRole) {
      // 合并到上一条消息
      const lastMsg = result[result.length - 1];
      if (Array.isArray(lastMsg.content) && Array.isArray(msg.content)) {
        lastMsg.content.push(...msg.content);
      }
    } else {
      result.push({ ...msg });
      lastRole = msg.role;
    }
  }

  // 确保以 user 开头
  if (result.length > 0 && result[0].role !== 'user') {
    result.unshift({
      role: 'user',
      content: [{ type: 'text', text: '[Conversation start]' }]
    });
  }

  return result;
}

// ==================== 【响应转换】OpenAI SSE → Claude SSE ====================

/**
 * 【响应转换】OpenAI → Claude SSE 响应发射器类
 * 用于将 OpenAI 流式响应转换为 Claude SSE 格式
 *
 * 转换方向: OpenAI SSE Stream → Claude SSE Stream
 */
export class OpenAIToClaudeSseEmitter {
  constructor(res, requestId, { model, inputTokens } = {}) {
    this.res = res;
    this.requestId = requestId || generateRequestId();
    this.model = model || 'claude-proxy';
    this.inputTokens = inputTokens || 0;
    this.nextIndex = 0;
    this.textBlockIndex = null;
    this.thinkingBlockIndex = null;
    this.finished = false;
    this.hasStarted = false;
    this.totalOutputTokens = 0;
  }

  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    writeSSE(this.res, 'message_start', buildMessageStartPayload(this.requestId, this.model, this.inputTokens));
  }

  ensureTextBlock() {
    // 容错机制：自动补发 message_start
    if (!this.hasStarted) this.start();
    if (this.textBlockIndex !== null) return;
    this.textBlockIndex = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.textBlockIndex,
      content_block: { type: 'text', text: '' }
    });
  }

  ensureThinkingBlock() {
    // 容错机制：自动补发 message_start
    if (!this.hasStarted) this.start();
    if (this.thinkingBlockIndex !== null) return;
    this.thinkingBlockIndex = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.thinkingBlockIndex,
      content_block: { type: 'thinking', thinking: '' }
    });
  }

  sendText(text) {
    if (!text) return;
    this.closeThinkingBlock();
    this.ensureTextBlock();
    this.totalOutputTokens += estimateTokensFromText(text);
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: this.textBlockIndex,
      delta: { type: 'text_delta', text }
    });
  }

  sendThinking(thinking) {
    if (!thinking) return;
    this.closeTextBlock();
    this.ensureThinkingBlock();
    this.totalOutputTokens += estimateTokensFromText(thinking);
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: this.thinkingBlockIndex,
      delta: { type: 'thinking_delta', thinking }
    });
  }

  async sendToolCalls(toolCalls = []) {
    if (!toolCalls || toolCalls.length === 0) return;
    await this.closeTextBlock();
    await this.closeThinkingBlock();

    toolCalls.forEach(call => {
      const index = this.nextIndex++;
      const args = call?.function?.arguments ?? '{}';
      const inputJson = typeof args === 'string' ? args : JSON.stringify(args);
      this.totalOutputTokens += estimateTokensFromText(inputJson);
      writeSSE(this.res, 'content_block_start', {
        type: 'content_block_start',
        index,
        content_block: {
          type: 'tool_use',
          id: call.id || generateToolUseId(),
          name: call?.function?.name || 'tool',
          input: {}
        }
      });
      // TASK-130: 增量发送 JSON 片段
      const CHUNK_SIZE = 128;
      for (let i = 0; i < inputJson.length; i += CHUNK_SIZE) {
        const chunk = inputJson.slice(i, i + CHUNK_SIZE);
        writeSSE(this.res, 'content_block_delta', {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: chunk }
        });
      }

      writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
    });
  }

  async closeTextBlock() {
    if (this.textBlockIndex === null) return;
    const index = this.textBlockIndex;
    this.textBlockIndex = null;
    writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
  }

  async closeThinkingBlock() {
    if (this.thinkingBlockIndex === null) return;
    const index = this.thinkingBlockIndex;
    this.thinkingBlockIndex = null;
    writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
  }

  finish(usage, finishReason = null) {
    if (this.finished) return;
    this.finished = true;
    this.closeTextBlock();
    this.closeThinkingBlock();

    const outputTokens =
      usage?.completion_tokens ??
      usage?.output_tokens ??
      (this.totalOutputTokens ?? 0);
    const inputTokens =
      usage?.prompt_tokens ??
      usage?.input_tokens ??
      (this.inputTokens ?? null);

    // 使用统一映射：如果传入了 OpenAI finishReason，则映射为 Claude stop_reason
    const stopReason = finishReason ? mapOpenAIFinishToClaude(finishReason) : 'end_turn';

    writeSSE(this.res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: {
        input_tokens: inputTokens || 0,
        output_tokens: outputTokens || 0
      }
    });
    writeSSE(this.res, 'message_stop', { type: 'message_stop' });
    this.res.end();
  }
}

// ==================== 导出 ====================

export {
  convertOpenAIImageToClaude,
  convertOpenAIFileToClaude,
  convertOpenAIContentToClaude,
  convertOpenAIToolCallsToClaude,
  convertOpenAIToolResultToClaude,
  convertOpenAIMessagesToClaude,
  convertOpenAIToolsToClaude,
  mapOpenAIToolChoiceToClaude
};
