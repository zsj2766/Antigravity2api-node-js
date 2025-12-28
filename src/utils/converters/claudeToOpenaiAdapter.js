/**
 * Claude → OpenAI 格式转换适配器
 *
 * 职责：
 * 1. 请求转换：Claude Messages API → OpenAI Chat Completions API
 * 2. 响应转换：OpenAI 响应 → Claude 格式（含 SSE 流式）
 * 3. 工具调用格式转换
 *
 * 支持的 Claude 内容类型：
 * - text: 纯文本
 * - image (base64/url): 图片
 * - document (base64/url): PDF/CSV 等文档
 * - tool_use: 工具调用 → OpenAI tool_calls（原生格式）
 * - tool_result: 工具结果 → OpenAI tool 消息（原生格式）
 * - thinking: 扩展思考
 * - redacted_thinking: 隐藏的思考内容
 */

import { generateRequestId, generateToolCallId, generateToolUseId } from '../idGenerator.js';
import {
  convertClaudeImageToOpenAI,
  extractMediaFromToolResult,
  resolveReasoningEffort
} from './common/index.js';
import { safeJsonStringify, safeJsonParse } from '../utils.js';

const THINKING_HINT = '<antml\\b:thinking_mode>interleaved</antml><antml\\b:max_thinking_length>16000</antml>';
const THINKING_START_TAG = '<thinking>';
const THINKING_END_TAG = '</thinking>';

// ==================== 请求转换：Claude → OpenAI ====================

/**
 * 将 Claude tool_use 块转换为 OpenAI tool_calls 格式
 * @param {Array} blocks - Claude 内容块数组
 * @returns {Array} - OpenAI tool_calls 数组
 */
function extractToolUsesAsOpenAIToolCalls(blocks) {
  if (!Array.isArray(blocks)) return [];

  return blocks
    .filter(b => b && b.type === 'tool_use')
    .map(b => ({
      id: b.id || generateToolCallId(),
      type: 'function',
      function: {
        name: b.name || 'unknown',
        arguments: safeJsonStringify(b.input, '{}')
      }
    }));
}

/**
 * 将 Claude 内容块转换为 OpenAI 格式的内容数组
 * 支持多模态内容（文本+图片）
 * 注意：tool_use 不再转为 XML，而是单独提取为 tool_calls
 */
function convertClaudeContentToOpenAI(content) {
  // 字符串内容直接返回
  if (typeof content === 'string') {
    return { content: content, toolCalls: [] };
  }

  if (!Array.isArray(content)) {
    return { content: '', toolCalls: [] };
  }

  const parts = [];
  let hasMultimodal = false;
  const toolCalls = extractToolUsesAsOpenAIToolCalls(content);

  for (const block of content) {
    if (!block || typeof block !== 'object') continue;

    switch (block.type) {
      case 'text':
        if (block.text && block.text.trim()) {
          parts.push({ type: 'text', text: block.text });
        }
        break;

      case 'thinking':
        // 将 thinking 内容转为带标签的文本
        if (block.thinking) {
          parts.push({ type: 'text', text: `${THINKING_START_TAG}${block.thinking}${THINKING_END_TAG}` });
        }
        break;

      case 'redacted_thinking':
        // 隐藏的思考内容，转为占位符
        parts.push({ type: 'text', text: '[redacted thinking]' });
        break;

      case 'tool_use':
        // 不再转为 XML，已通过 extractToolUsesAsOpenAIToolCalls 处理
        break;

      case 'tool_result':
        // tool_result 将在消息级别处理，转为单独的 tool 消息
        // 这里只提取可能的嵌套图片
        const mediaContent = extractMediaFromToolResult(block.content);
        if (mediaContent.images && mediaContent.images.length > 0) {
          hasMultimodal = true;
          for (const img of mediaContent.images) {
            if (img.inlineData) {
              parts.push({
                type: 'image_url',
                image_url: {
                  url: `data:${img.inlineData.mimeType};base64,${img.inlineData.data}`,
                  detail: 'auto'
                }
              });
            }
          }
        }
        break;

      case 'image':
        hasMultimodal = true;
        const openaiImage = convertClaudeImageToOpenAI(block);
        if (openaiImage) {
          parts.push(openaiImage);
        }
        break;

      case 'document':
        // OpenAI 目前不直接支持文档，转为文本占位符
        const docType = block.source?.media_type || 'application/pdf';
        parts.push({ type: 'text', text: `[Document: ${docType}]` });
        break;
    }
  }

  // 构建最终内容
  let finalContent;
  if (hasMultimodal) {
    finalContent = parts;
  } else if (parts.length === 0) {
    finalContent = '';
  } else {
    finalContent = parts
      .filter(p => p.type === 'text')
      .map(p => p.text)
      .join('\n');
  }

  return { content: finalContent, toolCalls };
}

/**
 * 从 Claude 消息中提取 tool_result 块
 */
function extractToolResults(content) {
  if (!Array.isArray(content)) return [];

  return content
    .filter(b => b && b.type === 'tool_result')
    .map(b => ({
      tool_use_id: b.tool_use_id,
      content: typeof b.content === 'string' ? b.content : JSON.stringify(b.content || ''),
      is_error: b.is_error
    }));
}

/**
 * 映射 Claude 角色到 OpenAI 角色
 */
function mapClaudeRole(role) {
  return role === 'assistant' ? 'assistant' : 'user';
}

/**
 * 将 Claude 请求体转换为 OpenAI 格式
 * 重构版：使用原生 tool_calls 替代 XML hack
 */
export function mapClaudeToOpenAI(body, triggerSignal) {
  if (!body || typeof body !== 'object') {
    throw new Error('请求体格式不合法');
  }
  if (typeof body.max_tokens !== 'number' || Number.isNaN(body.max_tokens)) {
    throw new Error('max_tokens 是必填数字');
  }
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    throw new Error('messages 不能为空');
  }

  const messages = [];

  // 处理 system 消息
  if (body.system) {
    const systemContent = Array.isArray(body.system)
      ? body.system
          .map(block => {
            if (typeof block === 'string') return block;
            if (block && typeof block === 'object' && 'text' in block) {
              return block.text || '';
            }
            return '';
          })
          .join('\n')
      : body.system;
    messages.push({ role: 'system', content: systemContent });
  }

  // 处理消息
  for (const message of body.messages) {
    if (message.role === 'user') {
      // 检查是否包含 tool_result
      const toolResults = extractToolResults(message.content);

      if (toolResults.length > 0) {
        // 将 tool_result 转为 OpenAI tool 消息
        for (const tr of toolResults) {
          let content = tr.content;
          // 处理 is_error，添加统一前缀
          if (tr.is_error) {
            content = `Error: ${content}`;
          }
          messages.push({
            role: 'tool',
            tool_call_id: tr.tool_use_id,
            content: content
          });
        }

        // 如果还有其他内容（非 tool_result），添加为用户消息
        const { content } = convertClaudeContentToOpenAI(message.content);
        if (content && (typeof content === 'string' ? content.trim() : content.length > 0)) {
          let finalContent = content;
          // 如果启用 thinking 且是用户消息，追加提示
          if (body.thinking && body.thinking.type === 'enabled') {
            if (typeof finalContent === 'string') {
              finalContent = `${finalContent}${THINKING_HINT}`;
            } else if (Array.isArray(finalContent)) {
              const lastTextIdx = finalContent.findLastIndex(p => p.type === 'text');
              if (lastTextIdx >= 0) {
                finalContent[lastTextIdx].text += THINKING_HINT;
              } else {
                finalContent.push({ type: 'text', text: THINKING_HINT });
              }
            }
          }
          messages.push({
            role: 'user',
            content: finalContent
          });
        }
      } else {
        // 普通用户消息
        let { content } = convertClaudeContentToOpenAI(message.content);

        // 如��启用 thinking，追加提示
        if (body.thinking && body.thinking.type === 'enabled') {
          if (typeof content === 'string') {
            content = `${content}${THINKING_HINT}`;
          } else if (Array.isArray(content)) {
            const lastTextIdx = content.findLastIndex(p => p.type === 'text');
            if (lastTextIdx >= 0) {
              content[lastTextIdx].text += THINKING_HINT;
            } else {
              content.push({ type: 'text', text: THINKING_HINT });
            }
          }
        }

        messages.push({
          role: 'user',
          content
        });
      }
    } else if (message.role === 'assistant') {
      // 助手消息：提取内容和工具调用
      const { content, toolCalls } = convertClaudeContentToOpenAI(message.content);

      const assistantMsg = {
        role: 'assistant',
        content: content || null
      };

      // 添加工具调用（原生格式）
      if (toolCalls.length > 0) {
        assistantMsg.tool_calls = toolCalls;
      }

      messages.push(assistantMsg);
    }
  }

  const result = {
    model: body.model,
    stream: body.stream !== false,
    temperature: body.temperature ?? 0.2,
    top_p: body.top_p ?? 1,
    max_tokens: body.max_tokens,
    messages
  };

  // 处理 thinking -> reasoning_effort (仅针对 o1/o3 系列模型)
  // 如果模型名称以 o1 或 o3 开头，且启用了 thinking，则转换 budget 为 reasoning_effort
  const isReasoningModel = typeof body.model === 'string' && /^(o1|o3)/.test(body.model);
  if (isReasoningModel && body.thinking && body.thinking.type === 'enabled') {
    if (body.thinking.budget_tokens) {
      result.reasoning_effort = resolveReasoningEffort(body.thinking.budget_tokens);
    }
  }

  // 添加工具定义
  if (body.tools && body.tools.length > 0) {
    result.tools = mapClaudeToolsToOpenAITools(body.tools);
    result.tool_choice = 'auto';
  }

  return result;
}

/**
 * 将 Claude 工具定义转换为 OpenAI 格式
 */
export function mapClaudeToolsToOpenAITools(tools = []) {
  if (!Array.isArray(tools)) return [];
  return tools.map(tool => ({
    type: 'function',
    function: {
      name: tool?.name,
      description: tool?.description,
      parameters: tool?.input_schema || {}
    }
  }));
}

// ==================== 响应转换：OpenAI → Claude ====================

/**
 * 将 OpenAI 工具调用转换为 Claude 格式块
 */
export function convertToolCallsToClaudeBlocks(toolCalls = []) {
  return (toolCalls || []).map(call => {
    const args = safeJsonParse(call?.function?.arguments, call?.function?.arguments || {});
    return {
      type: 'tool_use',
      id: call?.id || generateToolUseId(),
      name: call?.function?.name || 'tool',
      input: args || {}
    };
  });
}

/**
 * 估算文本 token 数量
 */
export function estimateTokensFromText(text) {
  if (!text) return 0;
  const normalized = typeof text === 'string' ? text : JSON.stringify(text);
  return Math.max(1, Math.ceil(normalized.length / 4));
}

/**
 * 从 Claude 消息中提取文本
 */
function extractTextFromClaudeMessages(messages = []) {
  return messages
    .map(msg => {
      if (typeof msg?.content === 'string') return msg.content;
      if (!Array.isArray(msg?.content)) return '';
      return msg.content
        .map(block => {
          if (!block || typeof block !== 'object') return '';
          if (block.type === 'text') return block.text || '';
          if (block.type === 'thinking') return block.thinking || '';
          if (block.type === 'tool_use') {
            return `[tool_use: ${block.name}]`;
          }
          if (block.type === 'tool_result') {
            return `[tool_result: ${block.tool_use_id}]`;
          }
          return '';
        })
        .join('');
    })
    .join('\n');
}

/**
 * 计算 Claude 请求的 token 数量
 */
export function countClaudeTokens(request) {
  if (!request || !Array.isArray(request.messages)) {
    throw new Error('messages 不能为空');
  }

  let totalText = extractTextFromClaudeMessages(request.messages);

  if (request.system) {
    const systemText = Array.isArray(request.system)
      ? request.system.map(block => (typeof block === 'string' ? block : block?.text || '')).join('\n')
      : request.system;
    totalText += `\n${systemText || ''}`;
  }

  if (request.tools && request.tools.length > 0) {
    totalText += `\n${JSON.stringify(request.tools)}`;
  }

  const inputTokens = estimateTokensFromText(totalText);

  return {
    input_tokens: inputTokens,
    token_count: inputTokens,
    tokens: inputTokens
  };
}

// ==================== SSE 响应构建 ====================

function buildMessageStartPayload(requestId, model, inputTokens = 0) {
  return {
    type: 'message_start',
    message: {
      id: `msg_${requestId}`,
      type: 'message',
      role: 'assistant',
      model: model || 'claude-proxy',
      stop_sequence: null,
      usage: {
        input_tokens: inputTokens || 0,
        output_tokens: 0
      },
      content: [],
      stop_reason: null
    }
  };
}

function writeSSE(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * Claude SSE 响应发射器类
 * 用于将 OpenAI 流式响应转换为 Claude SSE 格式
 */
export class ClaudeToOpenaiSseEmitter {
  constructor(res, requestId, { model, inputTokens } = {}) {
    this.res = res;
    this.requestId = requestId || generateRequestId();
    this.model = model || 'claude-proxy';
    this.inputTokens = inputTokens || 0;
    this.nextIndex = 0;
    this.textBlockIndex = null;
    this.thinkingBlockIndex = null;
    this.finished = false;
    this.totalOutputTokens = 0;
  }

  start() {
    writeSSE(this.res, 'message_start', buildMessageStartPayload(this.requestId, this.model, this.inputTokens));
  }

  ensureTextBlock() {
    if (this.textBlockIndex !== null) return;
    this.textBlockIndex = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.textBlockIndex,
      content_block: { type: 'text', text: '' }
    });
  }

  ensureThinkingBlock() {
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
      writeSSE(this.res, 'content_block_delta', {
        type: 'content_block_delta',
        index,
        delta: { type: 'input_json_delta', partial_json: inputJson }
      });
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

  finish(usage) {
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

    writeSSE(this.res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: 'end_turn', stop_sequence: null },
      usage: {
        input_tokens: inputTokens || 0,
        output_tokens: outputTokens || 0
      }
    });
    writeSSE(this.res, 'message_stop', { type: 'message_stop' });
    this.res.end();
  }
}

/**
 * 构建 Claude 内容块
 */
export function buildClaudeContentBlocks(content, toolCalls = []) {
  const blocks = [];
  if (content) {
    blocks.push({ type: 'text', text: content });
  }
  if (toolCalls && toolCalls.length > 0) {
    blocks.push(...convertToolCallsToClaudeBlocks(toolCalls));
  }
  return blocks;
}
