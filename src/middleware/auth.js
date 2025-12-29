/**
 * 认证中间件模块 (Authentication Middleware)
 *
 * 职责：
 * - 提供 API Key 认证功能（保护 /v1/* 端点）
 * - 提供管理面板会话认证功能
 * - 支持多种认证头格式
 *
 * 设计说明：
 * - 与业务逻辑解耦，可在路由层灵活组合使用
 * - 使用 sessionStore 单例管理会话状态
 * - 支持页面跳转和 API 响应两种认证失败处理方式
 *
 * @module middleware/auth
 */

import config from '../config/config.js';
import sessionStore from '../services/sessionStore.js';
import logger from '../utils/logger.js';

// ==================== API Key 认证 ====================

/**
 * 判断路径是否需要 API Key 保护
 *
 * 匹配以下模式的路径：
 * - /v1/*
 * - /{credential}/v1/*（凭证前缀模式）
 *
 * @param {string} pathname - 请求路径
 * @returns {boolean} 是否需要保护
 *
 * @example
 * isProtectedApiPath('/v1/chat/completions');     // true
 * isProtectedApiPath('/abc-123/v1/models');       // true
 * isProtectedApiPath('/admin/login');             // false
 */
export function isProtectedApiPath(pathname) {
  const normalized = pathname || '';
  return /^\/(?:[\w-]+\/)?v1\//.test(normalized);
}

/**
 * 从请求头中提取 API Key
 *
 * 按优先级检查以下来源：
 * 1. Authorization: Bearer {token}
 * 2. Authorization: {token}（无 Bearer 前缀）
 * 3. x-api-key / api-key / x-api_key / api_key 头
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {string|null} API Key 或 null
 *
 * @example
 * // Authorization: Bearer sk-xxx
 * extractApiKeyFromHeaders(req); // 'sk-xxx'
 */
export function extractApiKeyFromHeaders(req) {
  const headers = req.headers || {};
  const authHeader = headers.authorization;

  // Bearer Token 优先
  if (authHeader?.startsWith('Bearer ')) {
    return authHeader.slice(7);
  }
  if (authHeader) {
    return authHeader;
  }

  // 兼容各种大小写/横线/下划线写法
  const candidates = [
    headers['x-api-key'],
    headers['api-key'],
    headers['x-api_key'],
    headers['api_key']
  ];

  return candidates.find(v => v) || null;
}

/**
 * 验证 API Key 有效性
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {Object} 验证结果
 * @property {boolean} ok - 是否验证通过
 * @property {number} [status] - HTTP 状态码（验证失败时）
 * @property {string} [message] - 错误信息（验证失败时）
 *
 * @example
 * const result = validateApiKey(req);
 * if (!result.ok) {
 *   return res.status(result.status).json({ error: result.message });
 * }
 */
export function validateApiKey(req) {
  const apiKey = config.security?.apiKey;
  const providedKey = extractApiKeyFromHeaders(req);

  if (!apiKey) {
    return { ok: false, status: 503, message: 'API Key 未配置' };
  }

  if (!providedKey || providedKey !== apiKey) {
    return { ok: false, status: 401, message: 'Invalid API Key' };
  }

  return { ok: true };
}

/**
 * API Key 认证中间件
 *
 * 验证请求中的 API Key，失败时返回 JSON 错误响应。
 * 适用于需要显式保护的单个路由。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * app.get('/v1/models', requireApiKey, (req, res) => { ... });
 */
export function requireApiKey(req, res, next) {
  const result = validateApiKey(req);
  if (!result.ok) {
    logger.warn(`API Key 鉴权失败: ${req.method} ${req.originalUrl || req.url}`);
    return res.status(result.status).json({ error: result.message });
  }
  return next();
}

/**
 * 创建 API Key 保护中间件
 *
 * 返回一个中间件，自动保护匹配 /v1/* 模式的路径。
 * 适用于全局路由保护。
 *
 * @returns {Function} Express 中间件
 *
 * @example
 * app.use(createApiKeyGuard());
 */
export function createApiKeyGuard() {
  return (req, res, next) => {
    if (isProtectedApiPath(req.path)) {
      const result = validateApiKey(req);
      if (!result.ok) {
        logger.warn(`API Key 鉴权失败: ${req.method} ${req.path}`);
        return res.status(result.status).json({ error: result.message });
      }
    }
    next();
  };
}

// ==================== 管理面板认证 ====================

/**
 * 获取面板登录用户名
 *
 * @returns {string} 用户名，默认 'admin'
 */
export function getPanelUser() {
  return config.panelUser || 'admin';
}

/**
 * 检查面板密码是否已配置
 *
 * @returns {boolean} 是否已配置密码
 */
export function isPanelPasswordConfigured() {
  return !!config.panelPassword;
}

/**
 * 从请求中提取会话 Token
 *
 * 从 Cookie 中解析 panel_session 值。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {string|null} 会话 Token 或 null
 */
export function getSessionTokenFromReq(req) {
  const cookie = req.headers.cookie;
  if (!cookie) return null;

  const item = cookie
    .split(';')
    .map(s => s.trim())
    .find(c => c.startsWith('panel_session='));

  if (!item) return null;
  return decodeURIComponent(item.slice('panel_session='.length));
}

/**
 * 检查请求是否已通过面板认证
 *
 * 如果面板密码未配置，默认返回 true（允许访问）。
 * 否则验证会话 Token 的有效性。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {boolean} 是否已认证
 */
export function isPanelAuthed(req) {
  if (!isPanelPasswordConfigured()) return true;

  const token = getSessionTokenFromReq(req);
  if (!token) return false;

  return sessionStore.validate(token);
}

/**
 * 面板页面认证中间件
 *
 * 未认证时重定向到登录页面。
 * 适用于需要保护的管理页面路由。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * app.get('/admin/oauth', requirePanelAuthPage, (req, res) => { ... });
 */
export function requirePanelAuthPage(req, res, next) {
  if (!isPanelPasswordConfigured()) return next();
  if (isPanelAuthed(req)) return next();
  return res.redirect('/admin/login');
}

/**
 * 面板 API 认证中间件
 *
 * 未认证时返回 401 JSON 响应。
 * 适用于管理面板的 API 端点。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * app.get('/auth/accounts', requirePanelAuthApi, (req, res) => { ... });
 */
export function requirePanelAuthApi(req, res, next) {
  if (!isPanelPasswordConfigured()) return next();
  if (isPanelAuthed(req)) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

/**
 * 验证面板登录凭证
 *
 * @param {string} username - 用户名
 * @param {string} password - 密码
 * @returns {boolean} 凭证是否有效
 */
export function validatePanelCredentials(username, password) {
  return username === getPanelUser() && password === config.panelPassword;
}
