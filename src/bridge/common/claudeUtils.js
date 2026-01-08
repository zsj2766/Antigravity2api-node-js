/**
 * Claude 格式工具函数
 *
 * Token 估算、内容块构建等通用功能
 */

import { estimateTokens, generateToolUseId } from './idUtils.js';

/**
 * 安全解析 JSON
 * @param {string|object} str - JSON 字符串或对象
 * @param {*} fallback - 解析失败时的默认值
 * @returns {*} - 解析结果
 */
function safeJsonParse(str, fallback = {}) {
  if (typeof str !== 'string') return str ?? fallback;
  try {
    return JSON.parse(str);
  } catch {
    return fallback;
  }
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

  const inputTokens = estimateTokens(totalText);

  return {
    input_tokens: inputTokens,
    token_count: inputTokens,
    tokens: inputTokens
  };
}

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
