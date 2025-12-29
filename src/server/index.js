/**
 * 服务器主入口 (Server Entry Point)
 *
 * 职责：
 * - 初始化 Express 应用
 * - 配置全局中间件
 * - 注册路由模块
 * - 启动 HTTP 服务器
 * - 处理优雅关闭
 *
 * @module server
 */

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

// 配置
import config from '../config/config.js';
import logger from '../utils/logger.js';
import { closeRequester } from '../api/client.js';

// 中间件
import {
  isPanelAuthed,
  createApiKeyGuard
} from '../middleware/auth.js';
import { requestLogger } from '../middleware/logger.js';
import { entityTooLargeHandler } from '../middleware/errorHandler.js';

// 路由
import {
  adminRoutes,
  authRoutes,
  geminiRoutes,
  v1Routes,
  renderOAuthCallback
} from '../routes/index.js';

// 控制器 (仅用于健康检查)
import { healthCheck } from '../controllers/healthController.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ===== 环境变量安全检查 =====

if (!config.panelPassword) {
  logger.error(
    'PANEL_PASSWORD 环境变量未配置，出于安全考虑服务将不会启动，请在 Docker 环境变量中设置 PANEL_PASSWORD。'
  );
  process.exit(1);
}

if (!config.panelUser) {
  logger.error(
    'PANEL_USER 环境变量未配置，出于安全考虑服务将不会启动，请在 Docker 环境变量中设置 PANEL_USER。'
  );
  process.exit(1);
}

if (!config.security.apiKey) {
  logger.error(
    'API_KEY 环境变量未配置，出于安全考虑服务将不会启动，请在 Docker 环境变量中设置 API_KEY。'
  );
  process.exit(1);
}

// ===== Express 应用初始化 =====

const app = express();

// ===== 全局中间件 =====

// JSON 和 URL 编码解析
app.use(express.json({ limit: config.security.maxRequestSize }));
app.use(express.urlencoded({ extended: false }));

// 静态图片目录
app.use('/images', express.static(path.join(__dirname, '../../public/images')));

// 请求体大小错误处理
app.use(entityTooLargeHandler);

// 请求日志
app.use(requestLogger);

// API Key 保护
app.use(createApiKeyGuard());

// ===== 根路径 =====

app.get('/', (req, res) => {
  if (isPanelAuthed(req)) {
    return res.redirect('/admin/oauth');
  }
  return res.redirect('/admin/login');
});

// ===== 健康检查 =====

app.get('/healthz', healthCheck);

// ===== OAuth 回调 (多路径别名) =====

app.get(['/oauth-callback', '/auth/oauth/callback'], renderOAuthCallback);

// ===== 路由模块注册 =====

// 管理面板路由
app.use('/admin', adminRoutes);

// 认证路由 (OAuth + 账号管理)
app.use('/auth', authRoutes);

// OpenAI 兼容 API
app.use('/v1', v1Routes);

// Gemini API (多路径前缀)
app.use('/v1beta', geminiRoutes);
app.use('/gemini/v1beta', geminiRoutes);

// ===== 服务器启动 =====

const server = app.listen(config.server.port, config.server.host, () => {
  logger.info(`服务已启动: ${config.server.host}:${config.server.port}`);
});

server.on('error', error => {
  if (error.code === 'EADDRINUSE') {
    logger.error(`端口 ${config.server.port} 已被占用`);
    process.exit(1);
  } else if (error.code === 'EACCES') {
    logger.error(`端口 ${config.server.port} 无权限访问`);
    process.exit(1);
  } else {
    logger.error('服务启动失败:', error.message);
    process.exit(1);
  }
});

// ===== 优雅关闭 =====

const shutdown = () => {
  logger.info('正在关闭服务...');
  closeRequester();
  server.close(() => {
    logger.info('服务已关闭');
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 5000);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
