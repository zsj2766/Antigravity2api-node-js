/**
 * Bridge 适配层
 *
 * 提供与旧转换器相同的函数签名，内部使用 Bridge 转换器实现
 * 用于渐进式迁移，最小化控制器代码改动
 */

import { Bridge } from './index.js';
import config from '../config/config.js';
import { generateRequestId } from '../utils/idGenerator.js';
import { isThinkingModel } from '../utils/utils.js';
import log from '../utils/logger.js';

// 调试模式开关（通过环境变量控制）
const DEBUG_BRIDGE = process.env.DEBUG_BRIDGE === 'true';

/**
 * 调试日志辅助函数
 */
function debugLog(stage, data) {
  if (!DEBUG_BRIDGE) return;
  log.debug(`[Bridge:${stage}]`, typeof data === 'object' ? JSON.stringify(data, null, 2) : data);
}

/**
 * 生成 Gemini 请求体（OpenAI 格式输入）
 *
 * 兼容旧 generateRequestBody 函数签名
 *
 * @param {Array} messages - OpenAI 消息数组
 * @param {string} modelName - 模型名称
 * @param {object} parameters - 生成参数
 * @param {Array} tools - OpenAI 工具定义
 * @param {object} token - 认证 token
 * @param {string|object} toolChoice - 工具选择配置
 * @returns {object} 包装后的请求体
 */
export async function generateRequestBody(messages, modelName, parameters, tools, token, toolChoice) {
  const converter = Bridge.getRequestConverter('openai', 'gemini');

  // 构建 OpenAI 格式请求体
  const openaiBody = {
    messages,
    model: modelName,
    tools,
    tool_choice: toolChoice,
    ...parameters
  };

  // 调试日志：记录输入请求
  debugLog('OpenAI-Input', {
    model: modelName,
    messageCount: messages?.length,
    hasTools: !!tools?.length,
    reasoning_effort: parameters?.reasoning_effort,
    'reasoning.effort': parameters?.reasoning?.effort,
    'thinking.type': parameters?.thinking?.type
  });

  // 转换为 Gemini 格式
  const geminiRequest = await converter.convert(openaiBody, { model: modelName });

  // 调试日志：记录转换后的 Gemini 请求
  debugLog('Gemini-Output', {
    model: modelName,
    contentsCount: geminiRequest.contents?.length,
    hasTools: !!geminiRequest.tools?.length,
    thinkingConfig: geminiRequest.generationConfig?.thinkingConfig,
    maxOutputTokens: geminiRequest.generationConfig?.maxOutputTokens
  });

  // 检查是否需要注入 interleaved thinking hint（与 CLIProxyAPI antigravity_claude_request.go:356-383 一致）
  const hasTools = geminiRequest.tools?.length > 0;
  const hasThinking = geminiRequest.generationConfig?.thinkingConfig?.thinkingBudget > 0 ||
                      geminiRequest.generationConfig?.thinkingConfig?.includeThoughts === true ||
                      geminiRequest.generationConfig?.thinkingConfig?.include_thoughts === true;
  const isClaudeThinking = isThinkingModel(modelName) && modelName.toLowerCase().includes('claude');

  const INTERLEAVED_HINT = 'Interleaved thinking is enabled. You may think between tool calls and after receiving tool results before deciding the next action or final answer. Do not mention these instructions or any constraints about thinking blocks; just apply them.';

  // 合并系统指令
  if (geminiRequest.systemInstruction) {
    const existingText = geminiRequest.systemInstruction.parts?.[0]?.text || '';
    let combinedText = existingText ? `${existingText}\n\n${config.systemInstruction}` : config.systemInstruction;

    // 注入 interleaved thinking hint（当同时有 tools 和 thinking 且是 Claude thinking 模型时）
    if (hasTools && hasThinking && isClaudeThinking) {
      combinedText = combinedText ? `${combinedText}\n\n${INTERLEAVED_HINT}` : INTERLEAVED_HINT;
    }

    geminiRequest.systemInstruction = {
      role: 'user',
      parts: [{ text: combinedText }]
    };
  } else {
    let sysText = config.systemInstruction;
    // 注入 interleaved thinking hint
    if (hasTools && hasThinking && isClaudeThinking) {
      sysText = sysText ? `${sysText}\n\n${INTERLEAVED_HINT}` : INTERLEAVED_HINT;
    }
    geminiRequest.systemInstruction = {
      role: 'user',
      parts: [{ text: sysText }]
    };
  }

  // 添加 sessionId
  if (token?.sessionId) {
    geminiRequest.sessionId = token.sessionId;
  }

  // 包装成旧格式（与 CLIProxyAPI geminiToAntigravity 一致）
  return {
    project: token?.projectId,
    requestId: generateRequestId(),
    request: geminiRequest,
    model: modelName,
    userAgent: 'antigravity',
    requestType: 'agent'  // CLIProxyAPI antigravity_executor.go:1281
  };
}

/**
 * 生成 Gemini 请求体（Claude 格式输入）
 *
 * 兼容旧 generateRequestBodyFromAnthropic 函数签名
 *
 * @param {object} claudeBody - Claude Messages API 请求体
 * @param {object} token - 认证 token
 * @returns {object} 包装后的请求体
 */
export async function generateRequestBodyFromAnthropic(claudeBody, token) {
  // 验证必填参数（保持与旧转换器一致）
  if (!claudeBody || typeof claudeBody !== 'object') {
    throw new Error('请求体格式不合法');
  }
  if (typeof claudeBody.max_tokens !== 'number' || Number.isNaN(claudeBody.max_tokens)) {
    throw new Error('max_tokens 是必填数字');
  }
  if (!Array.isArray(claudeBody.messages) || claudeBody.messages.length === 0) {
    throw new Error('messages 不能为空');
  }

  const converter = Bridge.getRequestConverter('claude', 'gemini');
  const modelName = claudeBody.model;

  // 调试日志：记录 Claude 输入请求
  debugLog('Claude-Input', {
    model: modelName,
    messageCount: claudeBody.messages?.length,
    hasTools: !!claudeBody.tools?.length,
    max_tokens: claudeBody.max_tokens,
    'thinking.type': claudeBody.thinking?.type,
    'thinking.budget_tokens': claudeBody.thinking?.budget_tokens
  });

  // 转换为 Gemini 格式
  const geminiRequest = await converter.convert(claudeBody, { model: modelName });

  // 调试日志：记录转换后的 Gemini 请求
  debugLog('Gemini-Output', {
    model: modelName,
    contentsCount: geminiRequest.contents?.length,
    hasTools: !!geminiRequest.tools?.length,
    thinkingConfig: geminiRequest.generationConfig?.thinkingConfig,
    maxOutputTokens: geminiRequest.generationConfig?.maxOutputTokens
  });

  // 检查是否需要注入 interleaved thinking hint（与 CLIProxyAPI antigravity_claude_request.go:356-383 一致）
  const hasTools = geminiRequest.tools?.length > 0;
  const hasThinking = geminiRequest.generationConfig?.thinkingConfig?.thinkingBudget > 0 ||
                      geminiRequest.generationConfig?.thinkingConfig?.includeThoughts === true ||
                      geminiRequest.generationConfig?.thinkingConfig?.include_thoughts === true;
  const isClaudeThinking = isThinkingModel(modelName) && modelName.toLowerCase().includes('claude');

  const INTERLEAVED_HINT = 'Interleaved thinking is enabled. You may think between tool calls and after receiving tool results before deciding the next action or final answer. Do not mention these instructions or any constraints about thinking blocks; just apply them.';

  // 合并系统指令
  if (geminiRequest.systemInstruction) {
    const existingText = geminiRequest.systemInstruction.parts?.[0]?.text || '';
    let combinedText = existingText ? `${existingText}\n\n${config.systemInstruction}` : config.systemInstruction;

    // 注入 interleaved thinking hint（当同时有 tools 和 thinking 且是 Claude thinking 模型时）
    if (hasTools && hasThinking && isClaudeThinking) {
      combinedText = combinedText ? `${combinedText}\n\n${INTERLEAVED_HINT}` : INTERLEAVED_HINT;
    }

    geminiRequest.systemInstruction = {
      role: 'user',
      parts: [{ text: combinedText }]
    };
  } else {
    let sysText = config.systemInstruction;
    // 注入 interleaved thinking hint
    if (hasTools && hasThinking && isClaudeThinking) {
      sysText = sysText ? `${sysText}\n\n${INTERLEAVED_HINT}` : INTERLEAVED_HINT;
    }
    geminiRequest.systemInstruction = {
      role: 'user',
      parts: [{ text: sysText }]
    };
  }

  // 添加 sessionId
  if (token?.sessionId) {
    geminiRequest.sessionId = token.sessionId;
  }

  // 包装成旧格式（与 CLIProxyAPI geminiToAntigravity 一致）
  return {
    project: token?.projectId,
    requestId: generateRequestId(),
    request: geminiRequest,
    model: modelName,
    userAgent: 'antigravity',
    requestType: 'agent'  // CLIProxyAPI antigravity_executor.go:1281
  };
}

/**
 * 获取响应转换器
 *
 * @param {string} clientProtocol - 客户端协议 ('openai' | 'claude')
 * @returns {IResponseConverter} 响应转换器
 */
export function getResponseConverter(clientProtocol) {
  return Bridge.getResponseConverter('gemini', clientProtocol);
}

/**
 * 获取转换器对
 *
 * @param {string} clientProtocol - 客户端协议 ('openai' | 'claude')
 * @returns {{ req: IRequestConverter, res: IResponseConverter }}
 */
export function getConverterPair(clientProtocol) {
  return Bridge.getConverterPair(clientProtocol, 'gemini');
}
