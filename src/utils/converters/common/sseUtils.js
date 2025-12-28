/**
 * SSE 工具函数模块
 *
 * 提供 SSE 响应构建、Token 估算、Claude 格式消息处理等通用功能
 * 被 anthropicAdapter.js 和 claudeToOpenaiAdapter.js 共同使用
 */

import { safeJsonParse } from '../../utils.js';
import { generateToolUseId } from '../../idGenerator.js';

// ==================== Token 估算 ====================

/**
 * 估算文本 token 数量
 * @param {string|object} text - 文本或对象
 * @returns {number} - 估算的 token 数量
 */
export function estimateTokensFromText(text) {
  if (!text) return 0;
  const normalized = typeof text === 'string' ? text : JSON.stringify(text);
  return Math.max(1, Math.ceil(normalized.length / 4));
}

/**
 * 从 Claude 消息中提取文本内容
 * @param {Array} messages - Claude 消息数组
 * @returns {string} - 提取的文本
 */
export function extractTextFromClaudeMessages(messages = []) {
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
            return `<invoke name="${block.name}">${JSON.stringify(block.input || {})}</invoke>`;
          }
          if (block.type === 'tool_result') {
            return `<tool_result id="${block.tool_use_id}">${block.content ?? ''}</tool_result>`;
          }
          return '';
        })
        .join('');
    })
    .join('\n');
}

/**
 * 计算 Claude 请求的 token 数量
 * @param {object} request - Claude 请求对象
 * @returns {object} - token 统计信息
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

// ==================== 工具调用转换 ====================

/**
 * 将 OpenAI 工具调用转换为 Claude 格式块
 * @param {Array} toolCalls - OpenAI tool_calls 数组
 * @returns {Array} - Claude tool_use 块数组
 */
export function convertToolCallsToClaudeBlocks(toolCalls = []) {
  return (toolCalls || []).map(call => {
    const args = safeJsonParse(call?.function?.arguments);
    return {
      type: 'tool_use',
      id: call?.id || generateToolUseId(),
      name: call?.function?.name || 'tool',
      input: args || {}
    };
  });
}

/**
 * 构建 Claude 内容块
 * @param {string} content - 文本内容
 * @param {Array} toolCalls - 工具调用数组
 * @returns {Array} - Claude 内容块数组
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

// ==================== SSE 响应构建 ====================

/**
 * 构建 Claude message_start 事件的 payload
 * @param {string} requestId - 请求 ID
 * @param {string} model - 模型名称
 * @param {number} inputTokens - 输入 token 数量
 * @returns {object} - message_start payload
 */
export function buildMessageStartPayload(requestId, model, inputTokens = 0) {
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

/**
 * 写入 Claude 格式的 SSE 事件
 * @param {object} res - HTTP 响应对象
 * @param {string} event - 事件类型
 * @param {object} data - 事件数据
 */
export function writeSSE(res, event, data) {
  res.write(`event: ${event}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}
