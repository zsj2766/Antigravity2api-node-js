/**
 * 管理面板控制器 (Admin Controller)
 *
 * 职责：
 * - 处理管理面板的登录/登出
 * - 管理账号的 CRUD 操作
 * - 系统设置的读取和更新
 * - 调用日志的查询和管理
 * - 额度查询和统计
 *
 * 设计说明：
 * - 依赖 accountService 进行账号数据操作
 * - 依赖 tokenManager 进行凭证管理
 * - 依赖 sessionStore 进行会话管理
 * - 所有敏感操作需要 requirePanelAuthApi 中间件保护
 *
 * @module controllers/adminController
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import config, { updateEnvValues } from '../config/config.js';
import logger from '../utils/logger.js';
import sessionStore from '../services/sessionStore.js';
import {
  readAccountsSafe,
  readAccountsRaw,
  saveAccounts,
  normalizeTomlAccount,
  mergeAccounts
} from '../services/accountService.js';
import {
  SETTINGS_MAP,
  buildSettingsPayload
} from '../config/settings.js';
import { isDockerOnlyKey } from '../config/dataConfig.js';
import {
  getLogDetail,
  getRecentLogs,
  getUsageCountsWithinWindow,
  clearLogs,
  getDbStats,
  cleanupOldLogs,
  getLogCount,
  onLogAppended
} from '../utils/log_store.js';
import tokenManager from '../auth/token_manager.js';
import quotaManager from '../auth/quota_manager.js';
import { parseToml } from '../utils/tomlParser.js';
import { refreshApiClientConfig } from '../api/client.js';
import { resolveProjectIdFromAccessToken } from '../auth/project_id_resolver.js';
import { getPanelUser, isPanelPasswordConfigured, isPanelAuthed, getSessionTokenFromReq } from '../middleware/auth.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 注意：ACCOUNTS_FILE 路径应从 accountService.js 获取
// 此控制器通过 accountService 操作账号数据，不直接访问文件

/**
 * 检查面板认证是否启用
 * @returns {boolean} 是否启用密码保护
 */
const PANEL_AUTH_ENABLED = isPanelPasswordConfigured();

// ========== 登录/登出处理器 ==========

/**
 * 渲染登录页面
 *
 * 根据认证状态显示登录表单或重定向到管理面板。
 * 包含主题切换功能和用户友好的提示信息。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 */
export function renderLoginPage(req, res) {
  if (!PANEL_AUTH_ENABLED) {
    return res.send(
      '<h1>管理面板未启用登录</h1><p>未配置 PANEL_PASSWORD 环境变量，当前不启用面板密码保护。</p><p><a href="/admin/oauth">进入 OAuth 管理面板</a></p>'
    );
  }

  if (isPanelAuthed(req)) {
    return res.redirect('/admin/oauth');
  }

  const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <title>Antigravity 管理登录</title>
  <script>
    try {
      const saved = localStorage.getItem('ag-panel-theme');
      if (saved) {
        document.documentElement.setAttribute('data-theme', saved);
      }
    } catch (e) {}
  </script>
  <link rel="stylesheet" href="/admin/auth.css" />
</head>
<body>
  <div class="login-page">
    <div class="login-card">
      <h1>管理登录</h1>
      <p>登录后即可进入控制台进行授权、查看用量和配置。</p>
      <form class="login-form" method="POST" action="/admin/login">
        <label>用户名
          <input name="username" autocomplete="username" value="${config.panelUser || 'admin'}" />
        </label>
        <label>密码
          <input type="password" name="password" autocomplete="current-password" />
        </label>
        <div class="login-actions">
          <button type="submit">登录</button>
          <button type="button" id="loginThemeToggle" class="refresh-btn login-toggle">🌙 切换为暗色</button>
        </div>
        <div class="login-hint">用户名由环境变量 PANEL_USER 配置，密码由环境变量 PANEL_PASSWORD 配置。</div>
      </form>
    </div>
  </div>
  <script src="/admin/theme.js"></script>
  <script>
    window.AgTheme?.bindThemeToggle?.(document.getElementById('loginThemeToggle'));
  </script>
</body>
</html>`;

  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(html);
}

/**
 * 处理登录请求
 *
 * 验证用户名和密码，成功后创建会话并设置 Cookie。
 * 使用 sessionStore 管理会话生命周期。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 */
export function handleLogin(req, res) {
  if (!PANEL_AUTH_ENABLED) {
    return res.redirect('/admin/oauth');
  }

  const { username, password } = req.body || {};
  if (username === getPanelUser() && password === config.panelPassword) {
    const { token, maxAge } = sessionStore.create();
    res.setHeader(
      'Set-Cookie',
      `panel_session=${encodeURIComponent(token)}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${maxAge}`
    );
    return res.redirect('/admin/oauth');
  }

  return res
    .status(401)
    .send('<h1>登录失败</h1><p>用户名或密码错误。</p><p><a href="/admin/login">返回重试</a></p>');
}

/**
 * 处理登出请求
 *
 * 删除服务端会话并清除客户端 Cookie。
 * 支持 JSON 和 HTML 两种响应格式。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 */
export function handleLogout(req, res) {
  const token = getSessionTokenFromReq(req);
  if (token) {
    sessionStore.delete(token);
  }

  res.setHeader(
    'Set-Cookie',
    'panel_session=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0'
  );

  if (req.accepts('json')) {
    return res.json({ success: true });
  }

  return res.redirect('/admin/login');
}

// ========== 账号管理处理器 ==========

/**
 * 获取账号列表
 *
 * 返回安全的账号摘要（不含敏感信息如 refresh_token）。
 * 包含使用量统计信息。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getAccounts(req, res) {
  res.json({ accounts: readAccountsSafe() });
}

/**
 * 刷新所有账号凭证
 *
 * 遍历所有账号并尝试刷新 access_token。
 * 记录每个账号的刷新结果，失败时可能自动禁用账号。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function refreshAllAccounts(req, res) {
  try {
    const accounts = readAccountsRaw();
    if (accounts.length === 0) {
      return res.json({ success: true, refreshed: 0, failed: 0, total: 0, results: [] });
    }

    const results = [];
    let refreshed = 0;
    let failed = 0;

    for (let i = 0; i < accounts.length; i += 1) {
      const account = accounts[i];
      if (!account) continue;

      try {
        await tokenManager.refreshToken(account);
        refreshed += 1;
        results.push({ index: i, status: 'ok' });
      } catch (e) {
        const statusCode = e?.statusCode;
        if (statusCode === 403 || statusCode === 400) {
          account.enable = false;
        }

        failed += 1;
        results.push({ index: i, status: 'failed', error: e?.message || '刷新失败' });
        logger.warn(`账号 ${i + 1} 刷新失败: ${e?.message || e}`);
      }
    }

    saveAccounts(accounts);
    tokenManager.initialize();

    res.json({ success: true, refreshed, failed, total: accounts.length, results });
  } catch (e) {
    logger.error('批量刷新凭证失败', e.message);
    res.status(500).json({ error: e.message || '批量刷新失败' });
  }
}

/**
 * 获取凭证冻结历史
 *
 * 返回因错误或限流被临时冻结的账号记录。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getFreezeHistory(req, res) {
  res.json({ history: tokenManager.getFreezeHistory() });
}

/**
 * 刷新单个账号凭证
 *
 * 根据索引刷新指定账号的 access_token。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function refreshSingleAccount(req, res) {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) {
    return res.status(400).json({ error: '无效的账号序号' });
  }

  try {
    const accounts = readAccountsRaw();
    const target = accounts[index];
    if (!target) {
      return res.status(404).json({ error: '账号不存在' });
    }

    await tokenManager.refreshToken(target);
    saveAccounts(accounts);
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('刷新账号失败', e.message);
    res.status(500).json({ error: e.message || '刷新失败' });
  }
}

/**
 * 刷新单个账号的项目 ID
 *
 * 通过 Google Resource Manager API 重新获取项目 ID。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function refreshProjectId(req, res) {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) {
    return res.status(400).json({ error: 'invalid account index' });
  }

  try {
    const accounts = readAccountsRaw();
    const target = accounts[index];
    if (!target) {
      return res.status(404).json({ error: 'account not found' });
    }

    let accessToken = target.access_token;

    if (!accessToken && target.refresh_token) {
      try {
        await tokenManager.refreshToken(target);
        accessToken = target.access_token;
      } catch (err) {
        logger.error('failed to refresh token before resolving project id', err.message);
        return res.status(500).json({
          error: err?.message || 'failed to refresh token for this account'
        });
      }
    }

    if (!accessToken) {
      return res.status(400).json({
        error: 'no usable access token for this account'
      });
    }

    const result = await resolveProjectIdFromAccessToken(accessToken);
    if (!result.projectId) {
      const errorMessage = result.error?.message || 'failed to resolve project id from Resource Manager';
      logger.warn('refresh project id failed: unable to resolve project id from Resource Manager', errorMessage);
      return res.status(500).json({ error: errorMessage });
    }

    target.projectId = result.projectId;
    saveAccounts(accounts);
    tokenManager.initialize();

    return res.json({ success: true, projectId: result.projectId });
  } catch (e) {
    logger.error('refresh project id failed', e.message);
    return res.status(500).json({ error: e.message || 'refresh project id failed' });
  }
}

/**
 * 删除账号
 *
 * 根据索引删除指定账号。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function deleteAccount(req, res) {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) {
    return res.status(400).json({ error: '无效的账号序号' });
  }

  try {
    const accounts = readAccountsRaw();
    if (!accounts[index]) {
      return res.status(404).json({ error: '账号不存在' });
    }

    accounts.splice(index, 1);
    saveAccounts(accounts);
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('删除账号失败', e.message);
    res.status(500).json({ error: e.message || '删除失败' });
  }
}

/**
 * 切换账号启用状态
 *
 * 启用或禁用指定账号。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function toggleAccountEnable(req, res) {
  const index = Number.parseInt(req.params.index, 10);
  const { enable = true } = req.body || {};
  if (Number.isNaN(index)) {
    return res.status(400).json({ error: '无效的账号序号' });
  }

  try {
    const accounts = readAccountsRaw();
    if (!accounts[index]) {
      return res.status(404).json({ error: '账号不存在' });
    }

    accounts[index].enable = !!enable;
    saveAccounts(accounts);
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('更新账号状态失败', e.message);
    res.status(500).json({ error: e.message || '更新失败' });
  }
}

/**
 * 导入 TOML 格式账号
 *
 * 解析 TOML 内容并合并到现有账号列表。
 * 支持过滤禁用账号和完全替换模式。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function importTomlAccounts(req, res) {
  const {
    toml: tomlContent,
    replaceExisting = false,
    filterDisabled = true
  } = req.body || {};

  if (!tomlContent || typeof tomlContent !== 'string') {
    return res.status(400).json({ error: 'toml 字段必填且必须为字符串' });
  }

  let parsed;
  try {
    parsed = parseToml(tomlContent);
  } catch (e) {
    return res.status(400).json({ error: `TOML 解析失败: ${e.message}` });
  }

  const accountsFromToml = Array.isArray(parsed.accounts) ? parsed.accounts : [];
  if (accountsFromToml.length === 0) {
    return res.status(400).json({ error: '未在 TOML 中找到 accounts 列表' });
  }

  const normalized = [];
  let skipped = 0;

  for (const raw of accountsFromToml) {
    const acc = normalizeTomlAccount(raw, { filterDisabled });
    if (acc) {
      normalized.push(acc);
    } else {
      skipped += 1;
    }
  }

  if (normalized.length === 0) {
    return res.status(400).json({ error: 'TOML 中没有有效的账号信息' });
  }

  const existing = replaceExisting ? [] : readAccountsRaw();
  const merged = mergeAccounts(existing, normalized, replaceExisting);

  saveAccounts(merged);
  tokenManager.initialize();

  return res.json({
    success: true,
    imported: normalized.length,
    skipped,
    total: merged.length
  });
}

// ========== 设置管理处理器 ==========

/**
 * 获取系统设置
 *
 * 返回所有可配置项及其当前值。
 * 敏感值会被脱敏显示。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getSettings(req, res) {
  res.json(buildSettingsPayload());
}

/**
 * 更新系统设置
 *
 * 更新单个配置项的值。
 * 某些配置项更新后会触发即时生效逻辑。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function updateSettings(req, res) {
  const { key, value } = req.body || {};

  if (!key || typeof key !== 'string') {
    return res.status(400).json({ error: '缺少 key，无法更新配置' });
  }

  if (!SETTINGS_MAP.has(key)) {
    return res.status(400).json({ error: `不支持修改的配置项: ${key}` });
  }

  if (isDockerOnlyKey(key)) {
    return res.status(400).json({
      error: `此配置项 ${key} 为 Docker 专用，请在 docker-compose.yml 的 environment 部分修改`,
      dockerOnly: true
    });
  }

  try {
    const newConfig = updateEnvValues({ [key]: value ?? '' });

    // 特殊配置项的即时处理
    if (
      [
        'CREDENTIAL_MAX_USAGE_PER_HOUR',
        'CREDENTIAL_MAX_STICKY_USAGE',
        'CREDENTIAL_POOL_SIZE',
        'CREDENTIAL_COOLDOWN_MS'
      ].includes(key) &&
      typeof tokenManager.reloadConfig === 'function'
    ) {
      tokenManager.reloadConfig();
    }

    if (key === 'USE_NATIVE_AXIOS' && typeof refreshApiClientConfig === 'function') {
      refreshApiClientConfig();
    }

    return res.json({ success: true, ...buildSettingsPayload(newConfig) });
  } catch (e) {
    logger.error('更新环境变量失败', e.message || e);
    return res.status(500).json({ error: e.message || '更新配置失败' });
  }
}

/**
 * 获取面板配置
 *
 * 返回前端需要的配置信息（如 API Key）。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getPanelConfig(req, res) {
  res.json({ apiKey: config.security.apiKey || null });
}

// ========== 日志管理处理器 ==========

/**
 * 获取使用量统计
 *
 * 返回指定时间窗口内的 API 调用统计。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getUsageStats(req, res) {
  const windowMinutes = 60;
  const limitPerCredential = Number.isFinite(Number(tokenManager.hourlyLimit))
    ? Number(tokenManager.hourlyLimit)
    : null;
  const usage = getUsageCountsWithinWindow(windowMinutes * 60 * 1000);

  res.json({ windowMinutes, limitPerCredential, usage, updatedAt: new Date().toISOString() });
}

/**
 * 获取日志设置
 *
 * 返回调用日志的级别和保留策略配置。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getLogSettings(req, res) {
  const raw = (config.logging.requestLogLevel || '').toLowerCase();
  const level = ['off', 'error', 'all'].includes(raw) ? raw : 'all';

  const maxItems = config.logging.requestLogMaxItems;
  const retentionDays = config.logging.requestLogRetentionDays;

  res.json({ level, maxItems, retentionDays });
}

/**
 * 更新日志设置
 *
 * 设置调用日志的记录级别。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function updateLogSettings(req, res) {
  const { level } = req.body || {};
  const normalized = String(level || 'all').toLowerCase();

  if (!['off', 'error', 'all'].includes(normalized)) {
    return res.status(400).json({ error: 'REQUEST_LOG_LEVEL 只支持 off / error / all' });
  }

  try {
    updateEnvValues({ REQUEST_LOG_LEVEL: normalized });
    return res.json({ success: true, level: normalized });
  } catch (e) {
    logger.error('更新 REQUEST_LOG_LEVEL 失败', e.message || e);
    return res.status(500).json({ error: e.message || '更新调用日志配置失败' });
  }
}

/**
 * 获取调用日志列表
 *
 * 返回最近的 API 调用日志，支持分页和筛选。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getLogs(req, res) {
  const limit = req.query.limit ? Number.parseInt(req.query.limit, 10) : 50;
  const offset = req.query.offset ? Number.parseInt(req.query.offset, 10) : 0;
  const page = req.query.page ? Number.parseInt(req.query.page, 10) : 1;

  // 计算实际偏移量
  const actualOffset = offset || ((page - 1) * limit);

  // 筛选参数
  const options = {
    limit,
    offset: actualOffset,
    model: req.query.model || undefined,
    success: req.query.success !== undefined ? req.query.success === 'true' : undefined,
    projectId: req.query.projectId || undefined,
    startTime: req.query.startTime || undefined,
    endTime: req.query.endTime || undefined
  };

  // 获取总数用于分页
  const total = getLogCount(options);
  const logs = getRecentLogs(options);

  res.json({
    logs,
    pagination: {
      total,
      page: Math.floor(actualOffset / limit) + 1,
      limit,
      totalPages: Math.ceil(total / limit)
    }
  });
}

/**
 * 清空调用日志
 *
 * 删除所有存储的调用日志。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function clearAllLogs(req, res) {
  try {
    const ok = clearLogs();
    if (!ok) {
      return res.status(500).json({ error: '清空日志失败' });
    }
    return res.json({ success: true });
  } catch (e) {
    logger.error('清空调用日志失败:', e.message || e);
    return res.status(500).json({ error: e.message || '清空日志失败' });
  }
}

/**
 * 获取单条日志详情
 *
 * 根据日志 ID 返回完整的请求/响应详情。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getLogById(req, res) {
  const detail = getLogDetail(req.params.id);
  if (!detail) {
    return res.status(404).json({ error: '日志不存在或已过期' });
  }
  res.json({ log: detail });
}

// ========== 额度查询处理器 ==========

/**
 * 解析额度查询的索引参数
 *
 * @private
 * @param {string|Array} rawIndexes - 原始索引参数
 * @param {number} total - 账号总数
 * @returns {number[]|null} 解析后的索引数组
 */
function parseQuotaIndexes(rawIndexes, total) {
  if (rawIndexes === undefined || rawIndexes === null) return null;

  const normalized = Array.isArray(rawIndexes) ? rawIndexes.join(',') : String(rawIndexes);
  const candidates = normalized
    .split(/[,\s]+/)
    .map(part => parseInt(part, 10))
    .filter(num => Number.isFinite(num));

  const unique = [];
  candidates.forEach(num => {
    const zeroBased = num > 0 ? num - 1 : num;
    if (zeroBased >= 0 && zeroBased < total && !unique.includes(zeroBased)) {
      unique.push(zeroBased);
    }
  });

  return unique;
}

/**
 * 格式化额度查询响应
 *
 * @private
 * @param {Object} quotaResult - 原始额度数据
 * @returns {Object} 格式化后的响应
 */
function formatQuotaForResponse(quotaResult) {
  const quota = {};
  const models = quotaResult?.models || {};

  Object.entries(models).forEach(([modelId, info]) => {
    const remainingFraction = Number.isFinite(Number(info?.remaining))
      ? Number(info.remaining)
      : Number(info?.remainingFraction ?? 0);
    const modelQuota = { remainingFraction: remainingFraction || 0 };
    if (info?.resetTime) modelQuota.resetTime = info.resetTime;
    if (info?.resetTimeRaw) modelQuota.resetTimeRaw = info.resetTimeRaw;
    quota[modelId] = modelQuota;
  });

  return {
    code: '成功为200',
    msg: '成功就写获取成功',
    quota
  };
}

/**
 * 合并多个账号的额度数据
 *
 * @private
 * @param {Object} aggregate - 累积的额度对象
 * @param {Object} quotaMap - 单个账号的额度
 * @returns {Object} 合并后的额度对象
 */
function mergeQuota(aggregate, quotaMap) {
  Object.entries(quotaMap || {}).forEach(([modelId, info]) => {
    if (!aggregate[modelId]) {
      aggregate[modelId] = { remainingFraction: 0 };
      if (info.resetTime) aggregate[modelId].resetTime = info.resetTime;
      if (info.resetTimeRaw) aggregate[modelId].resetTimeRaw = info.resetTimeRaw;
    }
    const value = Number.isFinite(Number(info?.remainingFraction))
      ? Number(info.remainingFraction)
      : 0;
    aggregate[modelId].remainingFraction += value;
  });
  return aggregate;
}

/**
 * 获取启用凭证数量
 *
 * 用于外部 API 调用，通过 API Key 鉴权。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getQuotaList(req, res) {
  try {
    const accounts = readAccountsRaw();
    const enabled = accounts.filter(acc => acc && acc.enable !== false).length;
    return res.json({ code: '成功为200', msg: '成功就写获取成功', enabled });
  } catch (e) {
    logger.error('/admin/quota/list 获取启用凭证数量失败:', e.message);
    return res.status(500).json({ error: e.message || '获取启用凭证数量失败' });
  }
}

/**
 * 获取所有凭证的额度信息
 *
 * 查询指定或所有启用凭证的剩余额度。
 * 支持通过 ids/index/credentials 参数指定凭证。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getQuotaAll(req, res) {
  try {
    const accounts = readAccountsRaw();
    if (accounts.length === 0) {
      return res.status(404).json({ error: '暂无可用凭证' });
    }

    const indexes = parseQuotaIndexes(
      req.query.ids ?? req.query.index ?? req.query.credentials,
      accounts.length
    );
    const targetIndexes =
      indexes && indexes.length > 0
        ? indexes
        : accounts
          .map((_, idx) => idx)
          .filter(idx => accounts[idx]?.enable !== false);

    if (targetIndexes.length === 0) {
      return res.status(404).json({ error: '没有匹配的启用凭证' });
    }

    const payload = {};
    const aggregateQuota = {};

    for (const idx of targetIndexes) {
      const account = accounts[idx];
      const label = `凭证${idx + 1}`;

      if (!account || account.enable === false) {
        payload[label] = { code: '403', msg: '凭证未启用', quota: {} };
        continue;
      }

      if (!account.refresh_token) {
        payload[label] = { code: '400', msg: '凭证缺少 refresh_token', quota: {} };
        continue;
      }

      try {
        const quotaResult = await quotaManager.getQuotas(account.refresh_token, account);
        const formatted = formatQuotaForResponse(quotaResult);
        payload[label] = formatted;
        mergeQuota(aggregateQuota, formatted.quota);
      } catch (e) {
        payload[label] = {
          code: '500',
          msg: e.message || '获取额度失败',
          quota: {}
        };
      }
    }

    payload.all = {
      code: '成功为200',
      msg: '成功就写获取成功',
      quota: aggregateQuota
    };

    return res.json(payload);
  } catch (e) {
    logger.error('/admin/quota/all 获取额度失败:', e.message);
    return res.status(500).json({ error: e.message || '获取额度失败' });
  }
}

/**
 * 获取单个凭证的额度信息
 *
 * 根据索引查询指定凭证的剩余额度。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getSingleTokenQuota(req, res) {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) {
    return res.status(400).json({ error: '无效的凭证索引' });
  }

  try {
    const accounts = readAccountsRaw();
    if (index < 0 || index >= accounts.length) {
      return res.status(404).json({ error: '凭证不存在' });
    }

    const account = accounts[index];
    if (!account) {
      return res.status(404).json({ error: '凭证不存在' });
    }

    if (!account.refresh_token) {
      return res.status(400).json({ error: '凭证缺少 refresh_token' });
    }

    const quotaResult = await quotaManager.getQuotas(account.refresh_token, account);

    return res.json({
      success: true,
      data: quotaResult
    });
  } catch (e) {
    logger.error(`获取凭证 ${index} 额度失败:`, e.message);
    return res.status(500).json({ error: e.message || '获取额度失败' });
  }
}

/**
 * 获取 Token 运行时统计
 *
 * 返回每个凭证的使用次数、冷却状态等运行时信息。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getTokenStats(req, res) {
  try {
    const stats = {};
    if (tokenManager && Array.isArray(tokenManager.tokens)) {
      tokenManager.tokens.forEach((token) => {
        const key = token.projectId;
        if (!key) return;

        const s = tokenManager.getStats(token);
        stats[key] = {
          ...s,
          inCooldown: tokenManager.isInCooldown(token)
        };
      });
    }
    res.json({
      stats,
      config: {
        cooldownMs: tokenManager.cooldownMs,
        maxStickyUsage: tokenManager.MAX_STICKY_USAGE,
        poolSize: tokenManager.POOL_SIZE,
        hourlyLimit: tokenManager.hourlyLimit
      }
    });
  } catch (e) {
    logger.error('获取运行时统计失败:', e.message);
    res.status(500).json({ error: e.message || '获取运行时统计失败' });
  }
}

// ========== 数据库管理处理器 ==========

/**
 * 获取数据库统计信息
 *
 * 返回 SQLite 数据库的统计信息。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getDbStatsApi(req, res) {
  try {
    const stats = getDbStats();
    if (!stats) {
      return res.status(501).json({ error: '数据库统计不可用' });
    }
    return res.json({ success: true, stats });
  } catch (e) {
    logger.error('获取数据库统计失败:', e.message);
    return res.status(500).json({ error: e.message || '获取统计失败' });
  }
}

/**
 * 清理过期日志
 *
 * 删除超过保留期限的日志条目。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function cleanupLogsApi(req, res) {
  try {
    const deleted = cleanupOldLogs();
    return res.json({ success: true, deleted });
  } catch (e) {
    logger.error('清理过期日志失败:', e.message);
    return res.status(500).json({ error: e.message || '清理失败' });
  }
}

/**
 * 导出日志
 *
 * 以 JSON 格式导出日志数据。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function exportLogsApi(req, res) {
  try {
    const format = req.query.format || 'json';
    const limit = req.query.limit ? Number.parseInt(req.query.limit, 10) : 1000;

    const logs = getRecentLogs({ limit });

    if (format === 'csv') {
      // CSV 格式导出
      const headers = ['id', 'timestamp', 'model', 'projectId', 'success', 'status', 'durationMs', 'path', 'method', 'message'];
      const csvRows = [headers.join(',')];

      logs.forEach(log => {
        const row = headers.map(h => {
          const val = log[h];
          if (val === null || val === undefined) return '';
          if (typeof val === 'string' && (val.includes(',') || val.includes('"'))) {
            return `"${val.replace(/"/g, '""')}"`;
          }
          return String(val);
        });
        csvRows.push(row.join(','));
      });

      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', `attachment; filename="logs-${Date.now()}.csv"`);
      return res.send(csvRows.join('\n'));
    }

    // JSON 格式导出
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Content-Disposition', `attachment; filename="logs-${Date.now()}.json"`);
    return res.json({ logs, exportedAt: new Date().toISOString(), count: logs.length });
  } catch (e) {
    logger.error('导出日志失败:', e.message);
    return res.status(500).json({ error: e.message || '导出失败' });
  }
}

// ========== 日志文件管理 ==========

/**
 * 获取日志文件目录路径
 */
function getLogFilesDir() {
  return path.join(__dirname, '..', '..', 'data');
}

/**
 * 获取日志文件列表
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getLogFiles(req, res) {
  try {
    const logDir = getLogFilesDir();
    if (!fs.existsSync(logDir)) {
      return res.json({ files: [] });
    }

    const files = fs.readdirSync(logDir)
      .filter(name => {
        // 只列出日志相关文件
        const ext = path.extname(name).toLowerCase();
        return ['.db', '.json', '.log', '.txt'].includes(ext);
      })
      .map(name => {
        const filePath = path.join(logDir, name);
        try {
          const stat = fs.statSync(filePath);
          return {
            name,
            size: stat.size,
            sizeFormatted: formatFileSize(stat.size),
            modifiedAt: stat.mtime.toISOString(),
            modifiedAtFormatted: stat.mtime.toLocaleString('zh-CN')
          };
        } catch {
          return null;
        }
      })
      .filter(Boolean)
      .sort((a, b) => new Date(b.modifiedAt) - new Date(a.modifiedAt));

    return res.json({ files, directory: logDir });
  } catch (e) {
    logger.error('获取日志文件列表失败:', e.message);
    return res.status(500).json({ error: e.message || '获取文件列表失败' });
  }
}

/**
 * 格式化文件大小
 */
function formatFileSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

/**
 * 获取指定日志文件内容
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getLogFileContent(req, res) {
  try {
    const filename = req.params.filename;
    if (!filename || filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
      return res.status(400).json({ error: '无效的文件名' });
    }

    const filePath = path.join(getLogFilesDir(), filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: '文件不存在' });
    }

    const stat = fs.statSync(filePath);
    const ext = path.extname(filename).toLowerCase();

    // 对于大文件只返回部分内容
    const maxSize = 1024 * 1024; // 1MB
    let content = '';
    let truncated = false;

    if (ext === '.db') {
      // SQLite 数据库文件不支持直接预览
      return res.json({
        filename,
        size: stat.size,
        sizeFormatted: formatFileSize(stat.size),
        type: 'database',
        content: '[SQLite 数据库文件，不支持预览，请下载后使用数据库工具查看]',
        truncated: false
      });
    }

    if (stat.size > maxSize) {
      // 只读取前 1MB
      const fd = fs.openSync(filePath, 'r');
      const buffer = Buffer.alloc(maxSize);
      fs.readSync(fd, buffer, 0, maxSize, 0);
      fs.closeSync(fd);
      content = buffer.toString('utf-8');
      truncated = true;
    } else {
      content = fs.readFileSync(filePath, 'utf-8');
    }

    return res.json({
      filename,
      size: stat.size,
      sizeFormatted: formatFileSize(stat.size),
      modifiedAt: stat.mtime.toISOString(),
      type: ext === '.json' ? 'json' : 'text',
      content,
      truncated
    });
  } catch (e) {
    logger.error('读取日志文件失败:', e.message);
    return res.status(500).json({ error: e.message || '读取文件失败' });
  }
}

/**
 * 下载指定日志文件
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function downloadLogFile(req, res) {
  try {
    const filename = req.params.filename;
    if (!filename || filename.includes('..') || filename.includes('/') || filename.includes('\\')) {
      return res.status(400).json({ error: '无效的文件名' });
    }

    const filePath = path.join(getLogFilesDir(), filename);
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: '文件不存在' });
    }

    const stat = fs.statSync(filePath);
    const ext = path.extname(filename).toLowerCase();

    // 设置响应头
    res.setHeader('Content-Type', ext === '.json' ? 'application/json' : 'application/octet-stream');
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(filename)}"`);
    res.setHeader('Content-Length', stat.size);

    // 流式传输文件
    const stream = fs.createReadStream(filePath);
    stream.pipe(res);
  } catch (e) {
    logger.error('下载日志文件失败:', e.message);
    return res.status(500).json({ error: e.message || '下载失败' });
  }
}

// ========== SSE 实时日志推送 ==========

// 存储活跃的 SSE 连接
const sseClients = new Set();

/**
 * SSE 实时日志流
 *
 * 建立 Server-Sent Events 连接，实时推送新日志。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function liveLogsApi(req, res) {
  // 设置 SSE 头
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');

  // 发送初始连接成功消息
  res.write(`data: ${JSON.stringify({ type: 'connected', timestamp: new Date().toISOString() })}\n\n`);

  // 添加到客户端列表
  sseClients.add(res);

  // 心跳保持连接
  const heartbeat = setInterval(() => {
    res.write(`: heartbeat\n\n`);
  }, 30000);

  // 清理连接
  req.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
  });
}

/**
 * 广播日志到所有 SSE 客户端
 *
 * @param {Object} logEntry - 日志条目
 */
export function broadcastLog(logEntry) {
  if (sseClients.size === 0) return;

  const data = JSON.stringify({
    type: 'log',
    log: {
      id: logEntry.id,
      timestamp: logEntry.timestamp,
      model: logEntry.model,
      success: logEntry.success,
      status: logEntry.status,
      durationMs: logEntry.durationMs,
      path: logEntry.path,
      method: logEntry.method,
      message: logEntry.message
    }
  });

  sseClients.forEach(client => {
    try {
      client.write(`data: ${data}\n\n`);
    } catch {
      sseClients.delete(client);
    }
  });
}

// 注册日志监听器，自动广播到 SSE 客户端
onLogAppended(broadcastLog);

export default {
  // 登录/登出
  renderLoginPage,
  handleLogin,
  handleLogout,
  // 账号管理
  getAccounts,
  refreshAllAccounts,
  getFreezeHistory,
  refreshSingleAccount,
  refreshProjectId,
  deleteAccount,
  toggleAccountEnable,
  importTomlAccounts,
  // 设置管理
  getSettings,
  updateSettings,
  getPanelConfig,
  // 日志管理
  getUsageStats,
  getLogSettings,
  updateLogSettings,
  getLogs,
  clearAllLogs,
  getLogById,
  // 数据库管理
  getDbStatsApi,
  cleanupLogsApi,
  exportLogsApi,
  liveLogsApi,
  // 日志文件管理
  getLogFiles,
  getLogFileContent,
  downloadLogFile,
  broadcastLog,
  // 额度查询
  getQuotaList,
  getQuotaAll,
  getSingleTokenQuota,
  getTokenStats
};
