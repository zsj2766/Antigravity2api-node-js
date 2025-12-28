/**
 * 消息规范化工具
 *
 * 处理不同 API 格式间的消息结构差异：
 * - Claude 要求 user/assistant 严格交替
 * - Claude system 必须在顶层
 * - OpenAI 允许连续同角色消息
 */

import { generateRequestId } from '../../idGenerator.js';

/**
 * 从 OpenAI 消息数组中提取 system 消息
 * @param {Array} messages - OpenAI 格式消息数组
 * @returns {{ systemText: string, filteredMessages: Array }}
 */
function extractSystemMessages(messages) {
  const systemParts = [];
  const filteredMessages = [];

  for (const msg of messages) {
    if (msg.role === 'system') {
      if (typeof msg.content === 'string') {
        systemParts.push(msg.content);
      } else if (Array.isArray(msg.content)) {
        const text = msg.content
          .filter(p => p.type === 'text')
          .map(p => p.text || '')
          .join('\n');
        if (text) systemParts.push(text);
      }
    } else {
      filteredMessages.push(msg);
    }
  }

  return {
    systemText: systemParts.join('\n\n'),
    filteredMessages
  };
}

/**
 * 合并连续同角色消息（Claude 要求）
 * @param {Array} messages - 消息数组
 * @returns {Array} - 合并后的消息数组
 */
function mergeConsecutiveRoles(messages) {
  if (!Array.isArray(messages) || messages.length === 0) {
    return [];
  }

  const merged = [];
  let current = null;

  for (const msg of messages) {
    // 跳过空消息
    if (!msg || !msg.role) continue;

    // 规范化角色（Claude 只接受 user/assistant）
    const role = msg.role === 'assistant' ? 'assistant' : 'user';

    if (current && current.role === role) {
      // 合并内容
      current.content = mergeContent(current.content, msg.content);
    } else {
      // 保存当前消息，开始新消息
      if (current) {
        merged.push(current);
      }
      current = {
        role,
        content: normalizeContent(msg.content)
      };
    }
  }

  // 添加最后一条消息
  if (current) {
    merged.push(current);
  }

  return merged;
}

/**
 * 规范化内容为数组格式
 */
function normalizeContent(content) {
  if (typeof content === 'string') {
    return [{ type: 'text', text: content }];
  }
  if (Array.isArray(content)) {
    return content;
  }
  return [{ type: 'text', text: '' }];
}

/**
 * 合并两个内容
 */
function mergeContent(existing, newContent) {
  const existingArr = normalizeContent(existing);
  const newArr = normalizeContent(newContent);
  return [...existingArr, ...newArr];
}

/**
 * 确保消息以 user 开头（Claude 要求）
 * @param {Array} messages - 消息数组
 * @returns {Array} - 修正后的消息数组
 */
function ensureUserFirst(messages) {
  if (!Array.isArray(messages) || messages.length === 0) {
    return [{ role: 'user', content: [{ type: 'text', text: '' }] }];
  }

  if (messages[0].role !== 'user') {
    // 插入空用户消息
    return [
      { role: 'user', content: [{ type: 'text', text: '[Conversation start]' }] },
      ...messages
    ];
  }

  return messages;
}

/**
 * 从消息历史中通过 tool_call_id 查找函数名
 * 用于 OpenAI → Gemini 转换时，从 tool 消息回溯查找函数名
 *
 * @param {Array} messages - 消息数组
 * @param {string} toolCallId - 工具调用 ID
 * @returns {string} - 函数名，未找到返回空字符串
 */
function findFunctionNameByToolCallId(messages, toolCallId) {
  if (!Array.isArray(messages) || !toolCallId) return '';

  // 反向遍历查找包含对应 tool_call 的 assistant 消息
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role === 'assistant' && Array.isArray(msg.tool_calls)) {
      for (const tc of msg.tool_calls) {
        if (tc.id === toolCallId) {
          return tc.function?.name || '';
        }
      }
    }
  }

  return '';
}

/**
 * 生成确定性工具调用 ID
 * 用于 Gemini → OpenAI 时 Gemini 不返回 ID 的情况
 *
 * @param {string} name - 函数名
 * @param {object} args - 函数参数
 * @returns {string} - 格式: call_xxxxxxxx
 */
function generateDeterministicToolCallId(name, args) {
  const input = `${name}:${JSON.stringify(args || {})}`;
  // 简单的字符串哈希
  let hash = 0;
  for (let i = 0; i < input.length; i++) {
    const char = input.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  const hashStr = Math.abs(hash).toString(16).padStart(8, '0').substring(0, 8);
  return `call_${hashStr}`;
}

/**
 * 将 OpenAI 消息规范化为 Claude 格式
 * 完整的转换流程：提取 system → 合并连续角色 → 确保 user 开头
 *
 * @param {Array} messages - OpenAI 格式消息数组
 * @returns {{ system: string, messages: Array }}
 */
function normalizeMessagesForClaude(messages) {
  if (!Array.isArray(messages)) {
    return {
      system: '',
      messages: [{ role: 'user', content: [{ type: 'text', text: '' }] }]
    };
  }

  // 1. 提取 system 消息
  const { systemText, filteredMessages } = extractSystemMessages(messages);

  // 2. 合并连续同角色
  const merged = mergeConsecutiveRoles(filteredMessages);

  // 3. 确保 user 开头
  const normalized = ensureUserFirst(merged);

  return {
    system: systemText,
    messages: normalized
  };
}

/**
 * 将 Claude 消息规范化为 OpenAI 格式
 * @param {string} system - Claude system prompt
 * @param {Array} messages - Claude 格式消息数组
 * @returns {Array} - OpenAI 格式消息数组
 */
function normalizeMessagesForOpenAI(system, messages) {
  const result = [];

  // 添加 system 消息
  if (system) {
    result.push({ role: 'system', content: system });
  }

  // 转换消息
  for (const msg of messages || []) {
    result.push({
      role: msg.role,
      content: msg.content
    });
  }

  return result;
}

export {
  extractSystemMessages,
  mergeConsecutiveRoles,
  ensureUserFirst,
  findFunctionNameByToolCallId,
  generateDeterministicToolCallId,
  normalizeMessagesForClaude,
  normalizeMessagesForOpenAI,
  normalizeContent,
  mergeContent
};
