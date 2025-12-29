/**
 * 路由主入口 (Routes Index)
 *
 * 聚合所有子路由模块，提供统一的路由注册接口。
 *
 * @module routes
 */

import adminRoutes from './admin/index.js';
import authRoutes, { renderOAuthCallback } from './auth/index.js';
import geminiRoutes from './gemini/index.js';
import v1Routes from './v1/index.js';

export {
  adminRoutes,
  authRoutes,
  geminiRoutes,
  v1Routes,
  renderOAuthCallback
};

export default {
  adminRoutes,
  authRoutes,
  geminiRoutes,
  v1Routes
};
