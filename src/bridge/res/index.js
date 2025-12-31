/**
 * 响应转换器统一导出
 */

export { GeminiToOpenAIResponseConverter } from './GeminiToOpenAIResponseConverter.js';
export { GeminiToClaudeResponseConverter } from './GeminiToClaudeResponseConverter.js';
export { ClaudeToOpenAIResponseConverter } from './ClaudeToOpenAIResponseConverter.js';
export { OpenAIToClaudeResponseConverter } from './OpenAIToClaudeResponseConverter.js';

// 转换器映射表
export const ResponseConverters = {
  'gemini->openai': () => import('./GeminiToOpenAIResponseConverter.js').then(m => new m.GeminiToOpenAIResponseConverter()),
  'gemini->claude': () => import('./GeminiToClaudeResponseConverter.js').then(m => new m.GeminiToClaudeResponseConverter()),
  'claude->openai': () => import('./ClaudeToOpenAIResponseConverter.js').then(m => new m.ClaudeToOpenAIResponseConverter()),
  'openai->claude': () => import('./OpenAIToClaudeResponseConverter.js').then(m => new m.OpenAIToClaudeResponseConverter())
};
