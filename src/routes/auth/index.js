/**
 * 认证路由 (Auth Routes)
 *
 * 职责：
 * - OAuth 授权流程
 * - 账号管理 CRUD
 *
 * @module routes/auth
 */

import { Router } from 'express';

// 中间件
import { requirePanelAuthApi } from '../../middleware/auth.js';

// 控制器
import {
  getOAuthUrl,
  renderOAuthCallback,
  parseOAuthUrl
} from '../../controllers/oauthController.js';

import {
  getAccounts,
  refreshAllAccounts,
  getFreezeHistory,
  refreshSingleAccount,
  refreshProjectId,
  deleteAccount,
  toggleAccountEnable,
  importTomlAccounts
} from '../../controllers/adminController.js';

const router = Router();

// ===== OAuth 路由 =====
router.get('/oauth/url', requirePanelAuthApi, getOAuthUrl);
router.post('/oauth/parse-url', requirePanelAuthApi, parseOAuthUrl);

// ===== 账号管理路由 =====
router.post('/accounts/import-toml', requirePanelAuthApi, importTomlAccounts);
router.get('/accounts', requirePanelAuthApi, getAccounts);
router.post('/accounts/refresh-all', requirePanelAuthApi, refreshAllAccounts);
router.get('/accounts/freeze-history', requirePanelAuthApi, getFreezeHistory);
router.post('/accounts/:index/refresh', requirePanelAuthApi, refreshSingleAccount);
router.post('/accounts/:index/refresh-project-id', requirePanelAuthApi, refreshProjectId);
router.delete('/accounts/:index', requirePanelAuthApi, deleteAccount);
router.post('/accounts/:index/enable', requirePanelAuthApi, toggleAccountEnable);

export default router;

// OAuth 回调路由需要单独导出，因为它有多个路径别名
export { renderOAuthCallback };
