/**
 * OAuth 控制器 (OAuth Controller)
 *
 * 职责：
 * - 生成 Google OAuth 授权 URL
 * - 处理 OAuth 回调页面
 * - 解析回调 URL 并交换 token
 * - 自动获取项目 ID 和用户邮箱
 *
 * 设计说明：
 * - 使用"手动粘贴回调 URL"模式，避免网络环境限制
 * - 支持自定义项目 ID 和随机生成项目 ID
 * - 依赖 accountService 进行账号存储
 * - 依赖 tokenManager 刷新凭证
 *
 * @module controllers/oauthController
 */

import crypto from 'crypto';
import config from '../config/config.js';
import logger from '../utils/logger.js';
import { buildAuthUrl, exchangeCodeForToken } from '../auth/oauth_client.js';
import { resolveProjectIdFromAccessToken, fetchUserEmail } from '../auth/project_id_resolver.js';
import tokenManager from '../auth/token_manager.js';
import { generateProjectId } from '../utils/idGenerator.js';
import {
  readAccountsRaw,
  saveAccounts,
  addAccount,
  replaceAccount
} from '../services/accountService.js';

/**
 * OAuth 状态令牌
 *
 * 用于防止 CSRF 攻击，每次服务启动时生成新的随机值。
 * 客户端在授权请求中携带此值，回调时需要匹配。
 *
 * @constant {string}
 */
const OAUTH_STATE = crypto.randomUUID();

/**
 * 获取当前 OAuth State（仅供测试使用）
 *
 * @returns {string} 当前的 OAuth state 值
 */
export function getOAuthState() {
  return OAUTH_STATE;
}

/**
 * 获取 OAuth 授权 URL
 *
 * 生成完整的 Google OAuth 授权链接，包含必要的 scope 和 state 参数。
 * 前端获取此 URL 后引导用户跳转进行授权。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 *
 * @example
 * // GET /auth/oauth/url
 * // Response: { "url": "https://accounts.google.com/o/oauth2/v2/auth?..." }
 */
export function getOAuthUrl(req, res) {
  const redirectUri = `http://localhost:${config.server.port}/oauth-callback`;
  const url = buildAuthUrl(redirectUri, OAUTH_STATE);
  res.json({ url });
}

/**
 * 渲染 OAuth 回调提示页面
 *
 * 当用户完成 Google 授权后会被重定向到此页面。
 * 页面提示用户复制地址栏 URL 并粘贴到管理面板。
 *
 * 设计说明：
 * - 不在此处直接交换 token，避免网络环境限制
 * - 用户需要手动复制 URL，由 parseUrl 接口处理
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 */
export function renderOAuthCallback(req, res) {
  return res.send(
    '<!DOCTYPE html>' +
    '<html lang="zh-CN"><head><meta charset="utf-8" />' +
    '<title>授权回调 - 请复制地址栏 URL</title>' +
    '<style>body{font-family:system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#f9fafb;margin:0;padding:24px;color:#111827;}h1{font-size:20px;margin:0 0 12px;}p{margin:6px 0;}code{padding:2px 4px;background:#e5e7eb;border-radius:4px;}</style>' +
    '</head><body>' +
    '<h1>授权流程已返回回调地址</h1>' +
    '<p>请复制当前页面浏览器地址栏中的完整 URL，回到 <code>Antigravity</code> 管理面板，在"粘贴回调 URL"输入框中粘贴并提交。</p>' +
    '<p>提交后，服务端会解析 URL 中的 <code>code</code> 参数并完成账户添加。</p>' +
    '</body></html>'
  );
}

/**
 * 解析 OAuth 回调 URL 并交换 Token
 *
 * 处理用户粘贴的回调 URL：
 * 1. 解析 URL 中的 code 和 state 参数
 * 2. 验证 state 防止 CSRF 攻击
 * 3. 使用 code 交换 access_token 和 refresh_token
 * 4. 尝试自动获取项��� ID 和用户邮箱
 * 5. 保存账号到 accounts.json
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 *
 * @example
 * // POST /auth/oauth/parse-url
 * // Body: { "url": "http://localhost:3120/oauth-callback?code=xxx&state=yyy" }
 * // Response: { "success": true }
 */
export async function parseOAuthUrl(req, res) {
  const { url, replaceIndex, customProjectId, allowRandomProjectId } = req.body || {};

  // 验证 URL 参数
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url 字段必填且必须为字符串' });
  }

  // 解析 URL
  let parsed;
  try {
    parsed = new URL(url);
  } catch (e) {
    return res.status(400).json({ error: '无效的 URL，无法解析' });
  }

  // 提取 code 和 state
  const code = parsed.searchParams.get('code');
  const state = parsed.searchParams.get('state');

  if (!code) {
    return res.status(400).json({ error: 'URL 中缺少 code 参数' });
  }

  // 验证 state 防止 CSRF
  if (state && state !== OAUTH_STATE) {
    logger.warn('OAuth state mismatch in pasted URL, possible CSRF or wrong URL.');
    return res.status(400).json({ error: 'state 校验失败，请确认粘贴的是最新的授权回调地址' });
  }

  // 构造 redirect_uri（必须与授权请求时一致）
  const redirectUri = `http://localhost:${config.server.port}/oauth-callback`;

  try {
    // 交换 code 获取 token
    const tokenData = await exchangeCodeForToken(code, redirectUri);

    let projectId = null;
    let userEmail = null;
    let projectResolveError = null;

    // 处理项目 ID：优先使用用户自定义值
    if (customProjectId && typeof customProjectId === 'string' && customProjectId.trim()) {
      projectId = customProjectId.trim();
      logger.info(`使用用户自定义项目ID: ${projectId}`);
    } else if (tokenData?.access_token) {
      // 自动获取项目 ID 的逻辑
      try {
        // 获取用户邮箱
        userEmail = await fetchUserEmail(tokenData.access_token);
        logger.info(`成功获取用户邮箱: ${userEmail}`);

        // 使用 Resource Manager API 获取项目 ID
        const result = await resolveProjectIdFromAccessToken(tokenData.access_token);
        if (result.projectId) {
          projectId = result.projectId;
          logger.info(`通过Resource Manager获取到项目ID: ${projectId}`);
        } else {
          // 备用方案：使用 loadCodeAssist 方法
          const loadedProjectId = await tokenManager.fetchProjectId({
            access_token: tokenData.access_token
          });
          if (loadedProjectId !== undefined && loadedProjectId !== null) {
            projectId = loadedProjectId;
            logger.info(`备用方案获取到项目ID: ${projectId}`);
          }
        }
      } catch (err) {
        projectResolveError = err;
      }
    }

    // 处理无法获取项目 ID 的情况
    if (!projectId && !allowRandomProjectId) {
      const message =
        projectResolveError?.message ||
        '无法自动获取 Google 项目 ID，对应接口的访问可能出现 403 错误，请检查权限和 API 组件，或选择使用随机 projectId 再申请！';
      return res.status(400).json({ error: message, code: 'PROJECT_ID_MISSING' });
    }

    // 使用随机生成的项目 ID
    if (!projectId && allowRandomProjectId) {
      projectId = generateProjectId();
      logger.info(`使用随机生成的项目ID: ${projectId}`);
    }

    // 构造账号对象
    const account = {
      access_token: tokenData.access_token,
      refresh_token: tokenData.refresh_token,
      expires_in: tokenData.expires_in,
      timestamp: Date.now()
    };

    if (projectId) {
      account.projectId = projectId;
    }

    if (userEmail) {
      account.email = userEmail;
    }

    // 保存账号
    const accounts = readAccountsRaw();
    if (Number.isInteger(replaceIndex) && replaceIndex >= 0 && replaceIndex < accounts.length) {
      // 替换现有账号
      replaceAccount(replaceIndex, account);
    } else {
      // 添加新账号
      addAccount(account);
    }

    // 重新加载 TokenManager
    if (typeof tokenManager.initialize === 'function') {
      tokenManager.initialize();
    }

    logger.info('OAuth token 已保存');
    return res.json({ success: true });
  } catch (e) {
    logger.error('OAuth 交换 token 失败:', e.message);
    return res.status(500).json({ error: `交换 token 失败: ${e.message}` });
  }
}

export default {
  getOAuthState,
  getOAuthUrl,
  renderOAuthCallback,
  parseOAuthUrl
};
