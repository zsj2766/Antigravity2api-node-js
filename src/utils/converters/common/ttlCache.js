/**
 * TTL 缓存类
 *
 * 带有自动过期和容量限制的 Map 封装，
 * 用于解决全局 Map 缓存无限增长的内存泄漏问题。
 *
 * @module utils/converters/common/ttlCache
 */

/**
 * TTL 缓存
 * @template K, V
 */
export class TTLCache {
  /**
   * @param {Object} options - 配置选项
   * @param {number} [options.ttlMs=600000] - 缓存过期时间（毫秒），默认 10 分钟
   * @param {number} [options.maxSize=500] - 最大缓存条目数，超出后淘汰最旧的
   * @param {number} [options.cleanupInterval=60000] - 清理间隔（毫秒），默认 1 分钟
   */
  constructor(options = {}) {
    this.ttlMs = options.ttlMs ?? 24 * 60 * 60 * 1000;
    this.maxSize = options.maxSize ?? 500;
    this.cleanupInterval = options.cleanupInterval ?? 60 * 1000;

    /** @type {Map<K, { value: V, expiresAt: number }>} */
    this.cache = new Map();

    // 定期清理过期条目
    this._cleanupTimer = setInterval(() => this._cleanup(), this.cleanupInterval);
    // 允许进程正常退出
    if (this._cleanupTimer.unref) {
      this._cleanupTimer.unref();
    }
  }

  /**
   * 设置缓存
   * @param {K} key
   * @param {V} value
   */
  set(key, value) {
    // 如果已存在，先删除以更新插入顺序
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }
    // 超出容量时删除最旧的条目
    while (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
    }
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + this.ttlMs
    });
  }

  /**
   * 获取缓存值
   * @param {K} key
   * @returns {V | undefined}
   */
  get(key) {
    const entry = this.cache.get(key);
    if (!entry) return undefined;
    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return undefined;
    }
    return entry.value;
  }

  /**
   * 检查是否存在
   * @param {K} key
   * @returns {boolean}
   */
  has(key) {
    return this.get(key) !== undefined;
  }

  /**
   * 删除条目
   * @param {K} key
   * @returns {boolean}
   */
  delete(key) {
    return this.cache.delete(key);
  }

  /**
   * 清空缓存
   */
  clear() {
    this.cache.clear();
  }

  /**
   * 获取当前缓存大小
   * @returns {number}
   */
  get size() {
    return this.cache.size;
  }

  /**
   * 清理过期条目
   * @private
   */
  _cleanup() {
    const now = Date.now();
    for (const [key, entry] of this.cache) {
      if (now > entry.expiresAt) {
        this.cache.delete(key);
      }
    }
  }

  /**
   * 销毁缓存（停止定时器）
   */
  destroy() {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
    }
    this.cache.clear();
  }
}

export default TTLCache;
