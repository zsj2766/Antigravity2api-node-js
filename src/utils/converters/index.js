/**
 * 转换器模块统一入口
 *
 * 提供四个适配器：
 * - openaiAdapter: OpenAI ↔ Gemini（请求+响应）
 * - anthropicAdapter: Claude ↔ Gemini（请求+响应+SSE）
 * - claudeToOpenaiAdapter: Claude → OpenAI（请求转换 + 响应转换）
 * - openaiToClaudeAdapter: OpenAI → Claude（请求转换 + 响应转换）
 */

// OpenAI Adapter (OpenAI ↔ Gemini)
export {
  generateRequestBody,
  convertGeminiToOpenAIToolCall,
  convertToToolCallWithSignature,
  toOpenAiUsage,
  parseGeminiStreamToOpenAI,
  flushTextAccumulator,
  // 新增：Gemini → OpenAI 辅助函数
  convertGeminiInlineDataToOpenAI,
  convertGeminiFileDataToOpenAI,
  convertGeminiPartsToOpenAIContent
} from './openaiAdapter.js';

// Anthropic Adapter (Claude ↔ Gemini)
export {
  generateRequestBodyFromAnthropic,
  ClaudeSseEmitter,
  countClaudeTokens,
  estimateTokensFromText,
  buildClaudeContentBlocks,
  convertToolCallsToClaudeBlocks,
  // 新增：Gemini → Claude 辅助函数
  convertGeminiPartsToClaude,
  convertGeminiResponseToClaude
} from './anthropicAdapter.js';

// Claude → OpenAI Adapter（请求：Claude→OpenAI，响应：Claude→OpenAI SSE）
export {
  mapClaudeToOpenAI,
  mapClaudeToolsToOpenAITools,
  convertClaudeResponseToOpenAI,
  ClaudeToOpenAISseEmitter
} from './claudeToOpenaiAdapter.js';

// OpenAI → Claude Adapter（请求：OpenAI→Claude，响应：OpenAI→Claude SSE）
export {
  mapOpenAIToClaude,
  convertOpenAIImageToClaude,
  convertOpenAIContentToClaude,
  convertOpenAIToolCallsToClaude,
  convertOpenAIToolsToClaude,
  OpenAIToClaudeSseEmitter
} from './openaiToClaudeAdapter.js';

// Common Utilities - Stop Reason Mapping
export {
  mapGeminiStopReason,
  mapClaudeStopToOpenAI,
  mapOpenAIFinishToClaude,
  STOP_REASON_MAP
} from './stopReasonMapper.js';

// Common Utilities - Tool Converter
export { ToolConverter } from './common/toolConverter.js';

// Common Utilities - Content Converter
export {
  ContentConverter,
  convertGeminiToOpenAI,
  convertGeminiToClaude,
  convertClaudeToOpenAI,
  convertOpenAIToClaude,
  convertToolCallsToClaude,
  convertToolCallsToOpenAI
} from './common/contentConverter.js';

// Common Utilities - SSE Emitter Base Classes
export { BaseSseEmitter } from './common/BaseSseEmitter.js';
export { ClaudeProtocolEmitter } from './common/ClaudeProtocolEmitter.js';
export { OpenAIProtocolEmitter } from './common/OpenAIProtocolEmitter.js';
