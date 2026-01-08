/**
 * Bridge 内部公共模块
 */

// ID + Token 估算 + 字符串分块
export {
  generateRequestId,
  generateToolCallId,
  generateToolUseId,
  estimateTokens,
  safeChunkString,
  safeChunkByBytes
} from './idUtils.js';

// Schema 清理 + 消息合并
export { cleanJsonSchema, mergeConsecutiveRoles } from './schemaUtils.js';

// Thinking 配置 + Stop Reason 映射
export {
  resolveThinkingBudget,
  resolveReasoningEffort,
  normalizeThinkingBudget,
  getThinkingLimits,
  shouldUseThinkingLevel,
  MODEL_THINKING_LIMITS,
  mapGeminiStopReason,
  mapClaudeStopToOpenAI,
  mapOpenAIFinishToClaude
} from './mappingUtils.js';

// 媒体常量
export {
  DATA_URL_REGEX,
  DOCUMENT_MIME_TYPES,
  AUDIO_FORMAT_MIME,
  EXTENSION_MIME_MAP,
  ANTIGRAVITY_SYSTEM_PREFIX
} from './constants.js';

// SSE Emitter
export { BaseSseEmitter } from './BaseSseEmitter.js';
export { OpenAIProtocolEmitter } from './OpenAIProtocolEmitter.js';
export { ClaudeProtocolEmitter } from './ClaudeProtocolEmitter.js';
export { CallbackProtocolEmitter } from './CallbackProtocolEmitter.js';

// Claude 工具函数
export {
  countClaudeTokens,
  buildClaudeContentBlocks,
  convertToolCallsToClaudeBlocks,
  extractTextFromClaudeMessages
} from './claudeUtils.js';
