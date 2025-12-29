/**
 * 通用工具函数模块
 *
 * 职责：
 * - 思维签名缓存管理（Gemini thoughtSignature）
 * - Gemini 配置生成（generationConfig, requestBody）
 * - 模型检测辅助函数
 * - JSON 安全解析/序列化
 * - 网络工具
 */

import config from '../config/config.js';
import { generateRequestId } from './idGenerator.js';
import { TTLCache } from './converters/common/ttlCache.js';
import os from 'os';

// ==================== 思维签名缓存 ====================

// 全局思维签名缓存：用于记录 Gemini 返回的 thoughtSignature（工具调用与文本），
// 并在后续请求中复用，避免后端报缺失错误。
// 使用 TTLCache 替代 Map，10 分钟过期 + 最大 500 条，避免长期运行内存泄漏。
const thoughtSignatureMap = new TTLCache({ ttlMs: 10 * 60 * 1000, maxSize: 500 });
const textThoughtSignatureMap = new TTLCache({ ttlMs: 10 * 60 * 1000, maxSize: 1000 });

function registerThoughtSignature(id, thoughtSignature) {
  if (!id || !thoughtSignature) return;
  thoughtSignatureMap.set(id, thoughtSignature);
}

function getThoughtSignature(id) {
  if (!id) return undefined;
  return thoughtSignatureMap.get(id);
}

function normalizeTextForSignature(text) {
  if (typeof text !== 'string') return '';
  return text
    .replace(/<think>[\s\S]*?<\/think>/g, '')
    .replace(/!\[[^\]]*]\([^)]+\)/g, '')
    .replace(/\r\n/g, '\n')
    .trim();
}

function registerTextThoughtSignature(text, thoughtSignature) {
  if (!text || !thoughtSignature) return;
  const originalText = typeof text === 'string' ? text : String(text);
  const trimmed = originalText.trim();
  const normalized = normalizeTextForSignature(trimmed);
  const payload = { signature: thoughtSignature, text: originalText };
  if (originalText) {
    textThoughtSignatureMap.set(originalText, payload);
  }
  if (normalized) {
    textThoughtSignatureMap.set(normalized, payload);
  }
  if (trimmed && trimmed !== normalized) {
    textThoughtSignatureMap.set(trimmed, payload);
  }
}

function getTextThoughtSignature(text) {
  if (typeof text !== 'string' || !text.trim()) return undefined;
  if (textThoughtSignatureMap.has(text)) {
    return textThoughtSignatureMap.get(text);
  }
  const trimmed = text.trim();
  if (textThoughtSignatureMap.has(trimmed)) {
    return textThoughtSignatureMap.get(trimmed);
  }
  const normalized = normalizeTextForSignature(trimmed);
  if (!normalized) return undefined;
  return textThoughtSignatureMap.get(normalized);
}

// ==================== Gemini 配置生成 ====================

function generateGenerationConfig(parameters, enableThinking, actualModelName) {
  const generationConfig = {
    topP: parameters.top_p ?? config.defaults.top_p,
    topK: parameters.top_k ?? config.defaults.top_k,
    temperature: parameters.temperature ?? config.defaults.temperature,
    candidateCount: 1,
    maxOutputTokens: parameters.max_tokens ?? config.defaults.max_tokens,
    stopSequences: [
      "<|user|>",
      "<|bot|>",
      "<|context_request|>",
      "<|endoftext|>",
      "<|end_of_turn|>"
    ],
    thinkingConfig: {
      includeThoughts: enableThinking,
      thinkingBudget: enableThinking ? 1024 : 0
    }
  }
  if (enableThinking && actualModelName.includes("claude")) {
    delete generationConfig.topP;
  }
  return generationConfig
}

// ==================== 网络工具 ====================

function getDefaultIp() {
  const interfaces = os.networkInterfaces();
  if (interfaces.WLAN) {
    for (const inter of interfaces.WLAN) {
      if (inter.family === 'IPv4' && !inter.internal) {
        return inter.address;
      }
    }
  } else if (interfaces.wlan2) {
    for (const inter of interfaces.wlan2) {
      if (inter.family === 'IPv4' && !inter.internal) {
        return inter.address;
      }
    }
  }
  return '127.0.0.1';
}

// 将 Gemini 原生 GenerateContentRequest 直接包装为 AntigravityRequester 所需的请求体
// 这样可以对外暴露 Gemini 规范，而内部仍复用同一套后端调用链
function generateRequestBodyFromGemini(geminiRequest, modelName, token) {
  const actualModelName = modelName;

  // 是否启用思维链，沿用现有逻辑，避免行为不一致
  const baseEnableThinking =
    actualModelName.endsWith('-thinking') ||
    actualModelName === 'gemini-2.5-pro' ||
    actualModelName.startsWith('gemini-3-pro-') ||
    actualModelName === 'rev19-uic3-1p' ||
    actualModelName === 'gpt-oss-120b-medium';
  const enableThinking = baseEnableThinking && !actualModelName.includes('claude');

  const contents = Array.isArray(geminiRequest?.contents) ? geminiRequest.contents : [];

  const systemInstruction =
    geminiRequest?.systemInstruction && typeof geminiRequest.systemInstruction === 'object'
      ? geminiRequest.systemInstruction
      : {
          role: 'user',
          parts: [{ text: config.systemInstruction }]
        };

  const request = {
    contents,
    systemInstruction,
    tools: Array.isArray(geminiRequest?.tools) ? geminiRequest.tools : undefined,
    toolConfig: geminiRequest?.toolConfig,
    safetySettings: geminiRequest?.safetySettings,
    generationConfig:
      geminiRequest?.generationConfig ||
      generateGenerationConfig({}, enableThinking, actualModelName),
    sessionId: token.sessionId
  };

  return {
    project: token.projectId,
    requestId: generateRequestId(),
    request,
    model: actualModelName,
    userAgent: 'antigravity'
  };
}
// ==================== 模型检测辅助函数 ====================

/**
 * 检测是否为 Claude 系列模型
 * @param {string} modelName - 模型名称
 * @returns {boolean}
 */
function isClaudeModel(modelName) {
  return typeof modelName === 'string' && modelName.includes('claude');
}

/**
 * 检测是否为思维链模型
 * @param {string} modelName - 模型名称
 * @returns {boolean}
 */
function isThinkingModel(modelName) {
  if (typeof modelName !== 'string') return false;
  return (
    modelName.endsWith('-thinking') ||
    modelName === 'gemini-2.5-pro' ||
    modelName.startsWith('gemini-3-pro-') ||
    modelName === 'rev19-uic3-1p' ||
    modelName === 'gpt-oss-120b-medium'
  );
}

// ==================== JSON 安全解析/序列化 ====================

/**
 * 安全 JSON 解析
 * @param {string} raw - 原始字符串
 * @param {*} fallback - 解析失败时的默认值
 * @returns {*}
 */
function safeJsonParse(raw, fallback = {}) {
  if (typeof raw !== 'string') return raw ?? fallback;
  try {
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

/**
 * 安全 JSON 序列化
 * @param {*} value - 要序列化的值
 * @param {string} fallback - 序列化失败时的默认值
 * @returns {string}
 */
function safeJsonStringify(value, fallback = '{}') {
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch {
    return fallback;
  }
}

export {
  // ID 生成（从 idGenerator 重新导出）
  generateRequestId,
  // Gemini 请求体生成
  generateRequestBodyFromGemini,
  generateGenerationConfig,
  // 模型检测
  isClaudeModel,
  isThinkingModel,
  // JSON 工具
  safeJsonParse,
  safeJsonStringify,
  // 思维签名缓存
  registerThoughtSignature,
  registerTextThoughtSignature,
  getTextThoughtSignature,
  getThoughtSignature,
  // 网络工具
  getDefaultIp
}
