/**
 * 转换器模块统一入口
 *
 * 提供四个适配器：
 * - openaiAdapter: OpenAI ↔ Gemini（请求+响应）
 * - anthropicAdapter: Claude ↔ Gemini（请求+响应+SSE）
 * - claudeToOpenaiAdapter: Claude → OpenAI（请求转换+响应转换）
 * - openaiToClaudeAdapter: OpenAI → Claude（请求转换+响应转换）
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

// Claude → OpenAI Adapter
export {
  mapClaudeToOpenAI,
  mapClaudeToolsToOpenAITools,
  ClaudeToOpenaiSseEmitter,
  countClaudeTokens as countClaudeTokensForOpenAI,
  buildClaudeContentBlocks as buildClaudeContentBlocksForOpenAI
} from './claudeToOpenaiAdapter.js';

// OpenAI → Claude Adapter (新增)
export {
  mapOpenAIToClaude,
  convertClaudeResponseToOpenAI,
  OpenAISseEmitter,
  convertOpenAIImageToClaude,
  convertOpenAIContentToClaude,
  convertOpenAIToolCallsToClaude,
  convertOpenAIToolsToClaude
} from './openaiToClaudeAdapter.js';
