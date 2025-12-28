/**
 * OpenAI ↔ Claude 双向转换适配器
 *
 * 职责：
 * 1. 请求转换：OpenAI Chat Completions API → Claude Messages API
 * 2. 响应转换：Claude 响应 → OpenAI 格式（含 SSE 流式）
 * 3. 工具调用格式转换
 *
 * 支持的 OpenAI 内容类型：
 * - text: 纯文本
 * - image_url (base64/url): 图片
 * - tool_calls: 工具调用
 * - tool role: 工具结果
 */

import { generateRequestId, generateToolUseId } from '../idGenerator.js';
import { resolveThinkingBudget, mapClaudeToOpenAI } from './common/index.js';

import {
  normalizeMessagesForClaude,
  findFunctionNameByToolCallId
} from './common/messageUtils.js';
import { safeJsonParse, safeJsonStringify } from '../utils.js';

// ==================== 请求转换：OpenAI → Claude ====================

/**
 * 将 OpenAI image_url 转换为 Claude image source
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

      case 'input_text':
        // OpenAI Responses API 格式
        if (part.text) {
          blocks.push({ type: 'text', text: part.text });
        }
        break;

      case 'input_image':
        // OpenAI Responses API 格式
        const respImageBlock = convertOpenAIImageToClaude({ url: part.image_url });
        if (respImageBlock) {
          blocks.push(respImageBlock);
        }
        break;

      case 'input_file':
        // OpenAI Responses API 文档格式
        const fileBlock = convertOpenAIFileToClaude(part);
        if (fileBlock) {
          blocks.push(fileBlock);
        }
        break;

      case 'reasoning':
        // OpenAI Responses API reasoning 格式转换为 Claude thinking
        const thinkingText = Array.isArray(part.summary)
          ? part.summary.map(s => s.text || '').join('')
          : '';

        if (thinkingText) {
          const thinkingBlock = {
            type: 'thinking',
            thinking: thinkingText
          };
          // 如果有 signature 则透传
          if (part.signature) {
            thinkingBlock.signature = part.signature;
          }
          blocks.push(thinkingBlock);
        }
        break;
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
 * 将 OpenAI 工具定义转换为 Claude 格式
 * @param {Array} tools - OpenAI tools 数组
 * @returns {Array} - Claude tools 数组
 */
function convertOpenAIToolsToClaude(tools) {
  if (!Array.isArray(tools)) return [];

  return tools.map(tool => {
    if (tool.type === 'function' && tool.function) {
      return {
        name: tool.function.name,
        description: tool.function.description,
        input_schema: tool.function.parameters || { type: 'object', properties: {} }
      };
    }
    return null;
  }).filter(Boolean);
}

/**
 * 将 OpenAI 请求体转换为 Claude 格式
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
    max_tokens: body.max_tokens || 4096,
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
      result.max_tokens = budgetTokens + 4096;
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

// ==================== 响应转换：Claude → OpenAI ====================

/**
 * 将 Claude tool_use 块转换为 OpenAI tool_calls
 * @param {Array} blocks - Claude 内容块数组
 * @returns {Array} - OpenAI tool_calls 数组
 */
function convertClaudeToolUsesToOpenAI(blocks) {
  if (!Array.isArray(blocks)) return [];

  return blocks
    .filter(b => b.type === 'tool_use')
    .map(b => ({
      id: b.id,
      type: 'function',
      function: {
        name: b.name,
        arguments: safeJsonStringify(b.input, '{}')
      }
    }));
}

/**
 * 从 Claude 内容块中提取文本
 * @param {Array} blocks - Claude 内容块数组
 * @returns {string} - 合并后的文本
 */
function extractTextFromClaudeBlocks(blocks) {
  if (!Array.isArray(blocks)) return '';

  return blocks
    .filter(b => b.type === 'text')
    .map(b => b.text || '')
    .join('');
}

/**
 * 将 Claude 非流式响应转换为 OpenAI 格式
 * @param {object} claudeResponse - Claude 响应
 * @param {string} requestId - 请求 ID
 * @returns {object} - OpenAI 格式响应
 */
export function convertClaudeResponseToOpenAI(claudeResponse, requestId) {
  const content = claudeResponse.content || [];
  const text = extractTextFromClaudeBlocks(content);
  const toolCalls = convertClaudeToolUsesToOpenAI(content);

  const message = {
    role: 'assistant',
    content: text || null
  };

  if (toolCalls.length > 0) {
    message.tool_calls = toolCalls;
  }

  // 映射 stop_reason（使用统一映射模块）
  const finishReason = mapClaudeToOpenAI(claudeResponse.stop_reason);

  return {
    id: `chatcmpl-${requestId || generateRequestId()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: claudeResponse.model || 'claude-proxy',
    choices: [{
      index: 0,
      message,
      finish_reason: finishReason
    }],
    usage: {
      prompt_tokens: claudeResponse.usage?.input_tokens || 0,
      completion_tokens: claudeResponse.usage?.output_tokens || 0,
      total_tokens: (claudeResponse.usage?.input_tokens || 0) + (claudeResponse.usage?.output_tokens || 0)
    }
  };
}

// ==================== SSE 流式响应转换 ====================

/**
 * OpenAI SSE 响应发射器类
 * 用于将 Claude 流式响应转换为 OpenAI SSE 格式
 */
export class OpenAISseEmitter {
  constructor(res, requestId, { model } = {}) {
    this.res = res;
    this.requestId = requestId || generateRequestId();
    this.model = model || 'claude-proxy';
    this.finished = false;
    this.hasStarted = false;
    this.toolCallIndex = 0;
    this.currentToolCallId = null;
  }

  /**
   * 发送首个 chunk (包含 role: assistant)
   */
  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: { role: 'assistant', content: '' },
        finish_reason: null
      }]
    });
  }

  writeSSE(data) {
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  /**
   * 发送文本增量
   */
  sendTextDelta(text) {
    if (!text || this.finished) return;

    // 容错机制：自动补发首个 chunk
    if (!this.hasStarted) this.start();

    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: { content: text },
        finish_reason: null
      }]
    });
  }

  /**
   * 发送工具调用开始
   */
  sendToolCallStart(id, name) {
    if (this.finished) return;

    this.currentToolCallId = id;
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: {
          tool_calls: [{
            index: this.toolCallIndex,
            id: id,
            type: 'function',
            function: { name: name, arguments: '' }
          }]
        },
        finish_reason: null
      }]
    });
  }

  /**
   * 发送工具调用参数增量
   */
  sendToolCallArgumentsDelta(args) {
    if (this.finished) return;

    const argsStr = typeof args === 'string' ? args : JSON.stringify(args);
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: {
          tool_calls: [{
            index: this.toolCallIndex,
            function: { arguments: argsStr }
          }]
        },
        finish_reason: null
      }]
    });
  }

  /**
   * 完成当前工具调用，准备下一个
   */
  finishToolCall() {
    this.toolCallIndex++;
    this.currentToolCallId = null;
  }

  /**
   * 完成响应
   */
  finish(finishReason = 'stop', usage = null) {
    if (this.finished) return;
    this.finished = true;

    // 发送最终 chunk
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: {},
        finish_reason: finishReason
      }],
      usage: usage ? {
        prompt_tokens: usage.input_tokens || usage.prompt_tokens || 0,
        completion_tokens: usage.output_tokens || usage.completion_tokens || 0,
        total_tokens: (usage.input_tokens || usage.prompt_tokens || 0) + (usage.output_tokens || usage.completion_tokens || 0)
      } : undefined
    });

    // 发送 [DONE]
    this.res.write('data: [DONE]\n\n');
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
  convertClaudeToolUsesToOpenAI,
  extractTextFromClaudeBlocks
};
