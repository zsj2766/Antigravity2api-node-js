/**
 * 管理面板路由 (Admin Routes)
 *
 * 职责：
 * - 登录/登出页面和 API
 * - 账号管理 (通过 /auth/accounts 子路由)
 * - 设置管理
 * - 日志管理
 * - 额度查询
 * - 静态资源服务
 *
 * @module routes/admin
 */

import { Router } from 'express';
import path from 'path';
import express from 'express';
import { fileURLToPath } from 'url';

// 中间件
import {
  requirePanelAuthPage,
  requirePanelAuthApi,
  requireApiKey,
  isPanelAuthed
} from '../../middleware/auth.js';

// 控制器
import {
  renderLoginPage,
  handleLogin,
  handleLogout,
  getSettings,
  updateSettings,
  getPanelConfig,
  getUsageStats,
  getLogSettings,
  updateLogSettings,
  getLogs,
  clearAllLogs,
  getLogById,
  getQuotaList,
  getQuotaAll,
  getSingleTokenQuota,
  getTokenStats,
  getDbStatsApi,
  cleanupLogsApi,
  exportLogsApi,
  liveLogsApi
} from '../../controllers/adminController.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const router = Router();

// ===== 登录/登出 =====
router.get('/login', renderLoginPage);
router.post('/login', handleLogin);
router.post('/logout', handleLogout);

// ===== 设置管理 =====
router.get('/settings', requirePanelAuthApi, getSettings);
router.post('/settings', requirePanelAuthApi, updateSettings);
router.get('/panel-config', requirePanelAuthApi, getPanelConfig);

// ===== 日志管理 =====
router.get('/logs/usage', requirePanelAuthApi, getUsageStats);
router.get('/logs/settings', requirePanelAuthApi, getLogSettings);
router.post('/logs/settings', requirePanelAuthApi, updateLogSettings);
router.get('/logs/stats', requirePanelAuthApi, getDbStatsApi);
router.post('/logs/cleanup', requirePanelAuthApi, cleanupLogsApi);
router.get('/logs/export', requirePanelAuthApi, exportLogsApi);
router.get('/logs/live', requirePanelAuthApi, liveLogsApi);
router.get('/logs', requirePanelAuthApi, getLogs);
router.post('/logs/clear', requirePanelAuthApi, clearAllLogs);
router.get('/logs/:id', requirePanelAuthApi, getLogById);

// ===== 额度查询 =====
router.get('/quota/list', requireApiKey, getQuotaList);
router.get('/quota/all', requireApiKey, getQuotaAll);
router.get('/tokens/:index/quotas', requirePanelAuthApi, getSingleTokenQuota);
router.get('/tokens/stats', requirePanelAuthApi, getTokenStats);

// ===== OAuth 管理面板页面 =====
router.get('/oauth', requirePanelAuthPage, (req, res) => {
  const filePath = path.join(__dirname, '..', '..', '..', 'public', 'admin', 'index.html');
  res.sendFile(filePath);
});

// ===== 静态资源 =====
const adminStatic = express.static(path.join(__dirname, '..', '..', '..', 'public', 'admin'), {
  setHeaders: (res, filePath) => {
    // 禁用 JS/CSS 缓存，确保前端更新立即生效
    if (filePath.endsWith('.js') || filePath.endsWith('.css')) {
      res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
      res.setHeader('Pragma', 'no-cache');
    }
  }
});

// 登录页仍需访问的公共静态资源（如样式、主题脚本），不应被登录保护拦截
const publicAdminAssets = new Set(['/auth.css', '/panel.css', '/theme.js']);

/**
 * 静态资源中间件
 * 公共资源不需要认证，其他资源需要登录
 */
router.use((req, res, next) => {
  if (req.method === 'GET' && publicAdminAssets.has(req.path)) {
    return adminStatic(req, res, next);
  }

  // 复用页面级的鉴权逻辑，未登录则重定向到 /admin/login
  requirePanelAuthPage(req, res, err => {
    if (err) return next(err);
    return adminStatic(req, res, next);
  });
});

export default router;
