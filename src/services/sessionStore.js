/**
 * 会话存储模块 (Session Store)
 *
 * 职责：
 * - 管理面板登录会话的内存存储
 * - 提供会话的创建、验证、删除功能
 * - 自动清理过期会话
 *
 * 设计说明：
 * - 使用单例模式，确保全局唯一的会话存储
 * - 解耦 middleware/auth.js 和 controllers/adminController.js 的直接依赖
 * - 防止循环引用问题
 *
 * @module services/sessionStore
 */

import crypto from 'crypto';

/**
 * 默认会话过期时间：2 小时（毫秒）
 * @constant {number}
 */
const DEFAULT_SESSION_TTL_MS = 2 * 60 * 60 * 1000;

/**
 * 会话存储管理类
 *
 * 使用内存 Map 存储会话信息，适用于单实例部署。
 * 对于多实例部署，需替换为 Redis 等分布式存储。
 *
 * @class SessionStore
 */
class SessionStore {
  /**
   * 创建会话存储实例
   * @param {Object} options - 配置选项
   * @param {number} [options.ttl=7200000] - 会话过期时间（毫秒）
   * @param {number} [options.cleanupInterval=300000] - 清理间隔（毫秒），默认 5 分钟
   */
  constructor(options = {}) {
    /**
     * 会话存储 Map: token -> expiresAt (timestamp)
     * @type {Map<string, number>}
     * @private
     */
    this._sessions = new Map();

    /**
     * 会话过期时间（毫秒）
     * @type {number}
     * @private
     */
    this._ttl = options.ttl || DEFAULT_SESSION_TTL_MS;

    /**
     * 定期清理过期会话的定时器
     * @type {NodeJS.Timeout|null}
     * @private
     */
    this._cleanupTimer = null;

    // 启动定期清理
    const cleanupInterval = options.cleanupInterval || 5 * 60 * 1000;
    this._startCleanup(cleanupInterval);
  }

  /**
   * 创建新会话
   *
   * 生成随机 token 并存储到内存中，返回 token 供客户端使用。
   *
   * @returns {Object} 包含 token 和过期时间的对象
   * @property {string} token - 会话令牌（48 字符十六进制）
   * @property {number} expiresAt - 过期时间戳（毫秒）
   * @property {number} maxAge - 剩余有效期（秒），用于 Set-Cookie
   *
   * @example
   * const { token, expiresAt, maxAge } = sessionStore.create();
   * res.setHeader('Set-Cookie', `panel_session=${token}; Max-Age=${maxAge}`);
   */
  create() {
    const token = crypto.randomBytes(24).toString('hex');
    const expiresAt = Date.now() + this._ttl;
    this._sessions.set(token, expiresAt);

    return {
      token,
      expiresAt,
      maxAge: Math.floor(this._ttl / 1000)
    };
  }

  /**
   * 验证会话是否有效
   *
   * 检查 token 是否存在且未过期。
   * 如果已过期，自动从存储中删除。
   *
   * @param {string} token - 会话令牌
   * @returns {boolean} 会话是否有效
   *
   * @example
   * if (sessionStore.validate(token)) {
   *   // 用户已登录
   * }
   */
  validate(token) {
    if (!token) return false;

    const expiresAt = this._sessions.get(token);
    if (!expiresAt) return false;

    // 检查是否过期
    if (Date.now() > expiresAt) {
      this._sessions.delete(token);
      return false;
    }

    return true;
  }

  /**
   * 删除会话（用户登出）
   *
   * @param {string} token - 会话令牌
   * @returns {boolean} 是否成功删除
   */
  delete(token) {
    if (!token) return false;
    return this._sessions.delete(token);
  }

  /**
   * 获取当前活跃会话数量
   *
   * @returns {number} 活跃会话数
   */
  get size() {
    return this._sessions.size;
  }

  /**
   * 清理所有过期会话
   *
   * 遍历所有会话，删除已过期的条目。
   * 通常由定时器自动调用，也可手动触发。
   *
   * @returns {number} 清理的会话数量
   */
  cleanup() {
    const now = Date.now();
    let cleaned = 0;

    for (const [token, expiresAt] of this._sessions) {
      if (now > expiresAt) {
        this._sessions.delete(token);
        cleaned++;
      }
    }

    return cleaned;
  }

  /**
   * 启动定期清理任务
   *
   * @param {number} interval - 清理间隔（毫秒）
   * @private
   */
  _startCleanup(interval) {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
    }

    this._cleanupTimer = setInterval(() => {
      this.cleanup();
    }, interval);

    // 防止定时器阻止进程退出
    if (this._cleanupTimer.unref) {
      this._cleanupTimer.unref();
    }
  }

  /**
   * 停止清理任务并清空所有会话
   *
   * 用于测试或服务关闭时调用。
   */
  destroy() {
    if (this._cleanupTimer) {
      clearInterval(this._cleanupTimer);
      this._cleanupTimer = null;
    }
    this._sessions.clear();
  }
}

/**
 * 会话存储单例实例
 *
 * 全局共享的会话存储，用于管理面板登录状态。
 *
 * @type {SessionStore}
 * @example
 * import sessionStore from './services/sessionStore.js';
 *
 * // 创建会话
 * const { token, maxAge } = sessionStore.create();
 *
 * // 验证会话
 * const isValid = sessionStore.validate(token);
 *
 * // 删除会话
 * sessionStore.delete(token);
 */
const sessionStore = new SessionStore();

export { SessionStore, DEFAULT_SESSION_TTL_MS };
export default sessionStore;
