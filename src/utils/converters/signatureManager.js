/**
 * SignatureManager - ThoughtSignature 管理器
 *
 * 用于流式响应中的签名状态管理：
 * - 暂存从 Gemini part 收到的 thoughtSignature
 * - 在 thinking 块结束时消费并发送 signature_delta
 * - 处理空 text 带签名的 trailingSignature 场景
 */

import { TTLCache } from './common/ttlCache.js';

/**
 * 签名管理器类
 * 用于流式处理过程中暂存和消费 thoughtSignature
 */
export class SignatureManager {
  constructor() {
    this.pending = null;
  }

  /**
   * 存储签名
   * @param {string} signature - thoughtSignature 字符串
   */
  store(signature) {
    if (signature) this.pending = signature;
  }

  /**
   * 消费并返回签名（清空暂存）
   * @returns {string|null} - 暂存的签名，如果没有则返回 null
   */
  consume() {
    const sig = this.pending;
    this.pending = null;
    return sig;
  }

  /**
   * 检查是否有暂存的签名
   * @returns {boolean}
   */
  hasPending() {
    return !!this.pending;
  }
}

// ==================== tool_use.id -> thoughtSignature 缓存 ====================

/**
 * 全局缓存：tool_use.id -> thoughtSignature
 *
 * 规范要求：如果模型响应里出现 thoughtSignature，下一轮发送历史记录时必须原样带回到对应的 part。
 * 但 Claude Code 下一次请求不会回传 `tool_use.signature`（非标准字段），
 * 所以需要代理进程内维护一份 tool_use.id -> thoughtSignature 的映射，并在转回 v1internal 时补回。
 *
 * 使用 TTLCache 替代 Map，10 分钟过期 + 最大 500 条，避免长期运行内存泄漏。
 */
const toolThoughtSignatures = new TTLCache({ ttlMs: 10 * 60 * 1000, maxSize: 500 });

/**
 * 是否开启调试日志
 */
function isDebugEnabled() {
  const raw = process.env.AG2API_DEBUG;
  if (!raw) return false;
  const v = String(raw).trim().toLowerCase();
  return v === '1' || v === 'true' || v === 'yes' || v === 'on';
}

/**
 * 记录 tool_use.id 对应的 thoughtSignature
 * @param {string} toolUseId - 工具调用 ID
 * @param {string} thoughtSignature - 签名字符串
 */
export function rememberToolThoughtSignature(toolUseId, thoughtSignature) {
  if (!toolUseId || !thoughtSignature) return;
  const id = String(toolUseId);
  const sig = String(thoughtSignature);
  toolThoughtSignatures.set(id, sig);
  if (isDebugEnabled()) {
    console.log(`[ThoughtSignature] cached tool_use.id=${id} len=${sig.length}`);
  }
}

/**
 * 获取 tool_use.id 对应的 thoughtSignature
 * @param {string} toolUseId - 工具调用 ID
 * @returns {string|null} - 签名字符串，如果没有则返回 null
 */
export function getToolThoughtSignature(toolUseId) {
  if (!toolUseId) return null;
  const id = String(toolUseId);
  return toolThoughtSignatures.get(id) || null;
}
