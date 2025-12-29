/**
 * Web Search (Grounding) 工具函数
 *
 * 用于处理 Gemini 的 Grounding 响应，转换为 Claude 的 web_search 格式
 */

// ==================== 工具 ID 生成 ====================

/**
 * 生成 server_tool_use ID
 * @returns {string}
 */
export function makeSrvToolUseId() {
  return `srvtoolu_${Math.random().toString(36).slice(2, 26)}`;
}

// ==================== 加密内容生成 ====================

/**
 * 生成稳定的加密内容（用于 encrypted_content 和 encrypted_index 字段）
 * @param {object} payload - 要加密的内容
 * @returns {string} - Base64 编码的 JSON
 */
export function stableEncryptedContent(payload) {
  try {
    const json = JSON.stringify(payload);
    return Buffer.from(json, 'utf8').toString('base64');
  } catch {
    return '';
  }
}

// ==================== Grounding Chunks 转换 ====================

/**
 * 将 Gemini groundingChunks 转换为 Claude web_search_result 数组
 * @param {Array} groundingChunks - Gemini 的 groundingChunks 数组
 * @returns {Array} - Claude web_search_result 数组
 */
export function toWebSearchResults(groundingChunks = []) {
  return (groundingChunks || [])
    .map((chunk) => {
      const web = chunk?.web || {};
      const url = typeof web.uri === 'string' ? web.uri : '';
      const title = typeof web.title === 'string' ? web.title : (typeof web.domain === 'string' ? web.domain : '');
      return {
        type: 'web_search_result',
        title,
        url,
        encrypted_content: stableEncryptedContent({ url, title }),
        page_age: null,
      };
    })
    .filter((r) => r.url || r.title);
}

// ==================== Citation 构建 ====================

/**
 * 从 Gemini support 构建 Claude citation
 * @param {Array} results - web_search_result 数组
 * @param {object} support - Gemini 的 groundingSupport 对象
 * @returns {object|null} - Claude citation 对象
 */
export function buildCitationFromSupport(results, support) {
  const cited_text = support?.segment?.text;
  if (typeof cited_text !== 'string' || cited_text.length === 0) return null;

  const idx = Array.isArray(support?.groundingChunkIndices) ? support.groundingChunkIndices[0] : null;
  if (typeof idx !== 'number') return null;

  const result = results[idx];
  if (!result) return null;

  return {
    type: 'web_search_result_location',
    cited_text,
    url: result.url,
    title: result.title,
    encrypted_index: stableEncryptedContent({ url: result.url, title: result.title, cited_text }),
  };
}

// ==================== Redirect URL 解析 ====================

// 缓存已解析的重定向 URL
const resolvedRedirectUrlCache = new Map();

/**
 * 检查是否是 Vertex Grounding 重定向 URL
 * @param {string} url
 * @returns {boolean}
 */
export function isVertexGroundingRedirectUrl(url) {
  return (
    typeof url === 'string' &&
    url.startsWith('https://vertexaisearch.cloud.google.com/grounding-api-redirect/')
  );
}

/**
 * 获取重定向后的最终 URL
 * @param {string} url - 原始 URL
 * @param {number} timeoutMs - 超时时间（毫秒）
 * @returns {Promise<string>} - 最终 URL
 */
async function fetchFinalUrl(url, timeoutMs) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { method: 'HEAD', redirect: 'follow', signal: controller.signal });
    if (res && typeof res.url === 'string' && res.url) return res.url;
    return url;
  } catch (e) {
    // 某些服务器不支持 HEAD 请求，降级为 GET
    try {
      const res = await fetch(url, { method: 'GET', redirect: 'follow', signal: controller.signal });
      const finalUrl = res && typeof res.url === 'string' && res.url ? res.url : url;
      try {
        if (res?.body?.cancel) await res.body.cancel();
      } catch {}
      try {
        if (res?.body?.destroy) res.body.destroy();
      } catch {}
      return finalUrl;
    } catch {
      return url;
    }
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * 解析 Vertex Grounding 重定向 URL
 * @param {string} url - 重定向 URL
 * @returns {Promise<string>} - 最终 URL
 */
export async function resolveVertexGroundingRedirectUrl(url) {
  if (!isVertexGroundingRedirectUrl(url)) return url;
  const cached = resolvedRedirectUrlCache.get(url);
  if (typeof cached === 'string') return cached;
  if (cached && typeof cached.then === 'function') return cached;

  const promise = (async () => {
    const finalUrl = await fetchFinalUrl(url, 1500);
    return finalUrl;
  })();

  resolvedRedirectUrlCache.set(url, promise);
  try {
    const finalUrl = await promise;
    resolvedRedirectUrlCache.set(url, finalUrl);
    // 限制缓存大小
    if (resolvedRedirectUrlCache.size > 2000) resolvedRedirectUrlCache.clear();
    return finalUrl;
  } catch {
    resolvedRedirectUrlCache.delete(url);
    return url;
  }
}

/**
 * 批量解析 web search 结果中的重定向 URL
 * @param {object} webSearch - webSearch 状态对象（包含 results 数组）
 */
export async function resolveWebSearchRedirectUrls(webSearch) {
  const results = Array.isArray(webSearch?.results) ? webSearch.results : [];
  if (results.length === 0) return;

  // 最多解析前 10 个 URL
  await Promise.all(
    results.slice(0, 10).map(async (result) => {
      if (!result || typeof result.url !== 'string' || !result.url) return;
      const finalUrl = await resolveVertexGroundingRedirectUrl(result.url);
      if (finalUrl && finalUrl !== result.url) {
        result.url = finalUrl;
        result.encrypted_content = stableEncryptedContent({ url: result.url, title: result.title });
      }
    })
  );
}
