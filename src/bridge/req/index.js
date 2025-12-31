/**
 * 请求转换器统一导出
 */

export { OpenAIToGeminiRequestConverter } from './OpenAIToGeminiRequestConverter.js';
export { ClaudeToGeminiRequestConverter } from './ClaudeToGeminiRequestConverter.js';
export { OpenAIToClaudeRequestConverter } from './OpenAIToClaudeRequestConverter.js';
export { ClaudeToOpenAIRequestConverter } from './ClaudeToOpenAIRequestConverter.js';

// 转换器映射表
export const RequestConverters = {
  'openai->gemini': () => import('./OpenAIToGeminiRequestConverter.js').then(m => new m.OpenAIToGeminiRequestConverter()),
  'claude->gemini': () => import('./ClaudeToGeminiRequestConverter.js').then(m => new m.ClaudeToGeminiRequestConverter()),
  'openai->claude': () => import('./OpenAIToClaudeRequestConverter.js').then(m => new m.OpenAIToClaudeRequestConverter()),
  'claude->openai': () => import('./ClaudeToOpenAIRequestConverter.js').then(m => new m.ClaudeToOpenAIRequestConverter())
};
