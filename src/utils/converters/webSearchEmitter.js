/**
 * Web Search SSE 发射器
 *
 * 用于流式响应中发射 web_search 相关的 SSE 事件块
 * 将 Gemini Grounding 响应转换为 Claude 格式的 server_tool_use / web_search_tool_result / citations
 */

import { writeSSE } from './sseUtils.js';
import {
  makeSrvToolUseId,
  toWebSearchResults,
  buildCitationFromSupport,
  resolveWebSearchRedirectUrls
} from './webSearchUtils.js';

// ==================== Web Search 状态 ====================

/**
 * 创建 Web Search 状态对象
 * @returns {object} - 初始化的 webSearch 状态
 */
export function createWebSearchState() {
  return {
    toolUseId: null,
    query: '',
    results: [],
    supports: [],
    bufferedTextParts: [],
  };
}

// ==================== SSE 块发射 ====================

/**
 * 为 ClaudeSseEmitter 发射 web search 相关的所有块
 *
 * 流程：
 * 1. 关闭当前块
 * 2. server_tool_use 块（包含 query）
 * 3. web_search_tool_result 块（包含搜索结果）
 * 4. citations 块（每个 support 一个）
 * 5. 最终文本块
 *
 * @param {object} emitter - ClaudeSseEmitter 实例
 * @param {object} webSearch - webSearch 状态对象
 */
export async function emitWebSearchBlocksForEmitter(emitter, webSearch) {
  // 确保之前的块已关闭
  await emitter.closeThinkingBlock();
  await emitter.closeTextBlock();

  const toolUseId = webSearch.toolUseId || makeSrvToolUseId();
  const query = typeof webSearch.query === 'string' ? webSearch.query : '';
  const results = Array.isArray(webSearch.results) ? webSearch.results : [];
  const supports = Array.isArray(webSearch.supports) ? webSearch.supports : [];
  const bufferedTextParts = Array.isArray(webSearch.bufferedTextParts) ? webSearch.bufferedTextParts : [];

  // 1. server_tool_use 块
  const serverToolUseIndex = emitter.nextIndex++;
  writeSSE(emitter.res, 'content_block_start', {
    type: 'content_block_start',
    index: serverToolUseIndex,
    content_block: {
      type: 'server_tool_use',
      id: toolUseId,
      name: 'web_search',
      input: {},
    },
  });
  writeSSE(emitter.res, 'content_block_delta', {
    type: 'content_block_delta',
    index: serverToolUseIndex,
    delta: { type: 'input_json_delta', partial_json: JSON.stringify({ query }) },
  });
  writeSSE(emitter.res, 'content_block_stop', { type: 'content_block_stop', index: serverToolUseIndex });

  // 2. web_search_tool_result 块
  const toolResultIndex = emitter.nextIndex++;
  writeSSE(emitter.res, 'content_block_start', {
    type: 'content_block_start',
    index: toolResultIndex,
    content_block: {
      type: 'web_search_tool_result',
      tool_use_id: toolUseId,
      content: results,
    },
  });
  writeSSE(emitter.res, 'content_block_stop', { type: 'content_block_stop', index: toolResultIndex });

  // 3. citations 块（每个 support 一个）
  for (const support of supports) {
    const citation = buildCitationFromSupport(results, support);
    if (!citation) continue;

    const citationIndex = emitter.nextIndex++;
    writeSSE(emitter.res, 'content_block_start', {
      type: 'content_block_start',
      index: citationIndex,
      content_block: { type: 'text', text: '', citations: [] },
    });
    writeSSE(emitter.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: citationIndex,
      delta: { type: 'citations_delta', citation },
    });
    writeSSE(emitter.res, 'content_block_stop', { type: 'content_block_stop', index: citationIndex });
  }

  // 4. 最终文本块（输出缓存的非 thinking 文本）
  if (bufferedTextParts.length > 0) {
    const finalTextIndex = emitter.nextIndex++;
    writeSSE(emitter.res, 'content_block_start', {
      type: 'content_block_start',
      index: finalTextIndex,
      content_block: { type: 'text', text: '' },
    });
    for (const text of bufferedTextParts) {
      if (text) {
        writeSSE(emitter.res, 'content_block_delta', {
          type: 'content_block_delta',
          index: finalTextIndex,
          delta: { type: 'text_delta', text },
        });
      }
    }
    writeSSE(emitter.res, 'content_block_stop', { type: 'content_block_stop', index: finalTextIndex });
  }
}

// ==================== Grounding 数据提取 ====================

/**
 * 从 Gemini 响应候选中提取 grounding 数据
 * @param {object} candidate - Gemini 响应的 candidate 对象
 * @returns {object} - 提取的 grounding 数据
 */
export function extractGroundingData(candidate) {
  const groundingMetadata = candidate?.groundingMetadata || {};

  // 提取 webSearchQueries
  const webSearchQueries = groundingMetadata?.webSearchQueries;
  const query =
    Array.isArray(webSearchQueries) && typeof webSearchQueries[0] === 'string'
      ? webSearchQueries[0]
      : '';

  // 提取 groundingChunks（可能在 candidate 或 groundingMetadata 中）
  const groundingChunks = Array.isArray(candidate?.groundingChunks)
    ? candidate.groundingChunks
    : groundingMetadata?.groundingChunks;
  const results = Array.isArray(groundingChunks) ? toWebSearchResults(groundingChunks) : [];

  // 提取 groundingSupports
  const groundingSupports = Array.isArray(candidate?.groundingSupports)
    ? candidate.groundingSupports
    : groundingMetadata?.groundingSupports;
  const supports = Array.isArray(groundingSupports) ? groundingSupports : [];

  return { query, results, supports };
}

/**
 * 检查 candidate 是否包含 grounding 数据
 * @param {object} candidate - Gemini 响应的 candidate 对象
 * @returns {boolean}
 */
export function hasGroundingData(candidate) {
  if (!candidate) return false;
  return (
    Object.prototype.hasOwnProperty.call(candidate, 'groundingMetadata') ||
    Object.prototype.hasOwnProperty.call(candidate, 'groundingChunks') ||
    Object.prototype.hasOwnProperty.call(candidate, 'groundingSupports')
  );
}

// 重导出供外部使用
export { resolveWebSearchRedirectUrls, toWebSearchResults, buildCitationFromSupport, makeSrvToolUseId };
