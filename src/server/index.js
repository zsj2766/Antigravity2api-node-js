import express from 'express';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';
import { parseToml } from '../utils/tomlParser.js';
import {
  generateAssistantResponse,
  generateAssistantResponseNoStream,
  generateGeminiResponseNoStream,
  getAvailableModels,
  closeRequester,
  refreshApiClientConfig
} from '../api/client.js';
import { generateRequestBodyFromGemini } from '../utils/utils.js';
import {
  generateRequestBody,
  generateRequestBodyFromAnthropic,
  ClaudeSseEmitter,
  countClaudeTokens,
  buildClaudeContentBlocks,
  estimateTokensFromText,
  mapGeminiStopReason,
  mapOpenAIFinishReasonToClaude
} from '../utils/converters/index.js';
import { saveBase64Image } from '../utils/imageStorage.js';
import { generateProjectId } from '../utils/idGenerator.js';
import logger from '../utils/logger.js';
import {
  loadDataConfig,
  getEffectiveConfig as getEffectiveDataConfig,
  isDockerOnlyKey,
  getDockerOnlyKeys
} from '../config/dataConfig.js';
import config, { updateEnvValues } from '../config/config.js';
import tokenManager from '../auth/token_manager.js';
import { buildAuthUrl, exchangeCodeForToken } from '../auth/oauth_client.js';
import { resolveProjectIdFromAccessToken, fetchUserEmail } from '../auth/project_id_resolver.js';
import {
  appendLog,
  getLogDetail,
  getRecentLogs,
  getUsageCountsWithinWindow,
  getUsageSummary,
  clearLogs
} from '../utils/log_store.js';
import quotaManager from '../auth/quota_manager.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const ACCOUNTS_FILE = path.join(__dirname, '..', '..', 'data', 'accounts.json');
const OAUTH_STATE = crypto.randomUUID();
const PANEL_SESSION_TTL_MS = 2 * 60 * 60 * 1000; // 管理面板登录有效期：2 小时
const SENSITIVE_HEADERS = ['authorization', 'cookie'];

function getPanelUser() {
  return config.panelUser || 'admin';
}

function isPanelPasswordConfigured() {
  return !!config.panelPassword;
}

function sanitizeHeaders(headers = {}) {
  const result = {};
  Object.entries(headers || {}).forEach(([key, value]) => {
    result[key] = SENSITIVE_HEADERS.includes(String(key).toLowerCase()) ? '[REDACTED]' : value;
  });
  return result;
}

function createRequestSnapshot(req) {
  return {
    path: req.originalUrl,
    method: req.method,
    headers: sanitizeHeaders(req.headers),
    query: req.query,
    body: req.body
  };
}

function summarizeStreamEvents(events = []) {
  const summary = { text: '', tool_calls: null, thinking: '' };
  events.forEach(event => {
    if (event?.type === 'tool_calls') {
      summary.tool_calls = event.tool_calls;
    } else if (event?.type === 'thinking') {
      summary.thinking += event.content || '';
    } else if (event?.content) {
      summary.text += event.content;
    }
  });
  return summary;
}

function normalizeValue(value) {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return value;
}

function maskSecret(value) {
  if (value === undefined || value === null) return null;
  const str = String(value);
  if (!str) return null;
  if (str.length <= 4) return '****';
  return `${str.slice(0, 2)}${'*'.repeat(Math.max(4, str.length - 4))}${str.slice(-2)}`;
}

function buildSettingsSummary(configSnapshot = config) {
  const dataConfig = getEffectiveDataConfig();
  const configSource = configSnapshot || config;
  const groups = new Map();

  SETTINGS_DEFINITIONS.forEach(def => {
    // 使用统一配置获取逻辑，而不是直接读取process.env
    const envValue = process.env[def.key];
    const dataValue = dataConfig[def.key];
    const envNormalized = normalizeValue(envValue);
    const dataNormalized = normalizeValue(dataValue);
    const defaultNormalized = normalizeValue(def.defaultValue ?? null);

    // 判断配置来源：Docker环境变量 > data文件 > 默认值
    let source = 'default';
    let resolved = defaultNormalized;

    // Docker专用配置只能从环境变量读取
    if (isDockerOnlyKey(def.key)) {
      if (envValue !== undefined && envValue !== null && envValue !== '') {
        source = 'docker';
        resolved = normalizeValue(envValue);
      }
    } else {
      // 其他配置：data文件 > 默认值 (环境变量只是用于展示，不覆盖实际生效值)
      if (dataValue !== undefined && dataValue !== null && dataValue !== '') {
        source = 'file';
        resolved = dataNormalized;
      } else if (envValue !== undefined && envValue !== null && envValue !== '') {
        // 只有当data文件中没有值时才显示环境变量
        source = 'env';
        resolved = normalizeValue(envValue);
      }
    }

    const isDefault = source === 'default';

    const item = {
      key: def.key,
      label: def.label || def.key,
      value: def.sensitive ? maskSecret(resolved) : resolved,
      defaultValue: defaultNormalized,
      source,
      sensitive: !!def.sensitive,
      isDefault,
      isMissing: resolved === null,
      description: def.description || '',
      dockerOnly: isDockerOnlyKey(def.key) // 标记是否为Docker专用配置
    };

    const groupName = def.category || '未分组';
    if (!groups.has(groupName)) {
      groups.set(groupName, { name: groupName, items: [] });
    }
    groups.get(groupName).items.push(item);
  });

  return Array.from(groups.values());
}

const SETTINGS_DEFINITIONS = [
  {
    key: 'CREDENTIAL_MAX_USAGE_PER_HOUR',
    label: '凭证每小时调用上限',
    category: '限额与重试',
    defaultValue: 20,
    valueResolver: cfg => cfg.credentials.maxUsagePerHour
  },
  {
    key: 'CREDENTIAL_MAX_STICKY_USAGE',
    label: '连续调用保护次数',
    category: '限额与重试',
    defaultValue: 5,
    description: '同一凭证连续成功调用多少次后切换',
    valueResolver: cfg => cfg.credentials.maxStickyUsage
  },
  {
    key: 'CREDENTIAL_POOL_SIZE',
    label: '候选池大小',
    category: '限额与重试',
    defaultValue: 3,
    description: '从最久未使用的凭证中选取多少个作为候选',
    valueResolver: cfg => cfg.credentials.poolSize
  },
  {
    key: 'CREDENTIAL_COOLDOWN_MS',
    label: '冷却时间 (ms)',
    category: '限额与重试',
    defaultValue: 300000,
    description: '429 错误后的冷却时间（毫秒）',
    valueResolver: cfg => cfg.credentials.cooldownMs
  },
  {
    key: 'REQUEST_LOG_LEVEL',
    label: '调用日志级别',
    category: '调用日志',
    defaultValue: 'all',
    valueResolver: cfg => cfg.logging.requestLogLevel
  },
  {
    key: 'REQUEST_LOG_MAX_ITEMS',
    label: '调用日志最大保留条数',
    category: '调用日志',
    defaultValue: 200,
    valueResolver: cfg => cfg.logging.requestLogMaxItems
  },
  {
    key: 'REQUEST_LOG_RETENTION_DAYS',
    label: '调用日志保留天数',
    category: '调用日志',
    defaultValue: 7,
    valueResolver: cfg => cfg.logging.requestLogRetentionDays
  },
  {
    key: 'PANEL_USER',
    label: '面板登录用户名',
    category: '面板与安全',
    defaultValue: 'admin',
    valueResolver: () => getPanelUser()
  },
  {
    key: 'PANEL_PASSWORD',
    label: '面板登录密码',
    category: '面板与安全',
    defaultValue: null,
    sensitive: true,
    valueResolver: () => (isPanelPasswordConfigured() ? '已配置' : null),
    description: '用于保护管理界面，未配置将拒绝启动'
  },
  {
    key: 'API_KEY',
    label: 'API 密钥',
    category: '面板与安全',
    defaultValue: null,
    sensitive: true,
    valueResolver: cfg => cfg.security.apiKey || null,
    description: '保护 /v1/* 端点的访问'
  },
  {
    key: 'MAX_REQUEST_SIZE',
    label: '最大请求体',
    category: '面板与安全',
    defaultValue: '50mb',
    valueResolver: cfg => cfg.security.maxRequestSize
  },
  {
    key: 'PORT',
    label: '服务端口',
    category: '服务与网络',
    defaultValue: 8045,
    valueResolver: cfg => cfg.server.port
  },
  {
    key: 'HOST',
    label: '监听地址',
    category: '服务与网络',
    defaultValue: '0.0.0.0',
    valueResolver: cfg => cfg.server.host,
  },
  {
    key: 'API_URL',
    label: '流式接口 URL',
    category: '服务与网络',
    defaultValue:
      'https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:streamGenerateContent?alt=sse',
    valueResolver: cfg => cfg.api.url
  },
  {
    key: 'API_MODELS_URL',
    label: '模型列表 URL',
    category: '服务与网络',
    defaultValue: 'https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:fetchAvailableModels',
    valueResolver: cfg => cfg.api.modelsUrl
  },
  {
    key: 'API_NO_STREAM_URL',
    label: '非流式接口 URL',
    category: '服务与网络',
    defaultValue:
      'https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:generateContent',
    valueResolver: cfg => cfg.api.noStreamUrl
  },
  {
    key: 'API_HOST',
    label: 'API Host 头',
    category: '服务与网络',
    defaultValue: 'daily-cloudcode-pa.sandbox.googleapis.com',
    valueResolver: cfg => cfg.api.host
  },
  {
    key: 'API_USER_AGENT',
    label: 'User-Agent',
    category: '服务与网络',
    defaultValue: 'antigravity/1.11.3 windows/amd64',
    valueResolver: cfg => cfg.api.userAgent
  },
  {
    key: 'PROXY',
    label: 'HTTP 代理',
    category: '服务与网络',
    defaultValue: null,
    valueResolver: cfg => cfg.proxy
  },
  {
    key: 'TIMEOUT',
    label: '请求超时(ms)',
    category: '服务与网络',
    defaultValue: 180000,
    valueResolver: cfg => cfg.timeout
  },
  {
    key: 'USE_NATIVE_AXIOS',
    label: '使用原生 Axios',
    category: '服务与网络',
    defaultValue: 'false',
    valueResolver: cfg => cfg.useNativeAxios
  },
  {
    key: 'DEFAULT_TEMPERATURE',
    label: '默认温度',
    category: '生成参数',
    defaultValue: 1,
    valueResolver: cfg => cfg.defaults.temperature
  },
  {
    key: 'DEFAULT_TOP_P',
    label: '默认 top_p',
    category: '生成参数',
    defaultValue: 0.85,
    valueResolver: cfg => cfg.defaults.top_p
  },
  {
    key: 'DEFAULT_TOP_K',
    label: '默认 top_k',
    category: '生成参数',
    defaultValue: 50,
    valueResolver: cfg => cfg.defaults.top_k
  },
  {
    key: 'DEFAULT_MAX_TOKENS',
    label: '默认最大 Tokens',
    category: '生成参数',
    defaultValue: 8096,
    valueResolver: cfg => cfg.defaults.max_tokens
  },
  {
    key: 'SYSTEM_INSTRUCTION',
    label: '系统提示词',
    category: '生成参数',
    defaultValue: '',
    valueResolver: cfg => cfg.systemInstruction
  },
  {
    key: 'RETRY_STATUS_CODES',
    label: '重试状态码',
    category: '限额与重试',
    defaultValue: '429,500',
    valueResolver: cfg => cfg.retry.statusCodes
  },
  {
    key: 'RETRY_MAX_ATTEMPTS',
    label: '最大重试次数',
    category: '限额与重试',
    defaultValue: 3,
    valueResolver: cfg => cfg.retry.maxAttempts
  },
  {
    key: 'MAX_IMAGES',
    label: '图片保存上限',
    category: '限额与重试',
    defaultValue: 10,
    valueResolver: cfg => cfg.maxImages
  },
  {
    key: 'IMAGE_BASE_URL',
    label: '图片访问基础 URL',
    category: '限额与重试',
    defaultValue: null,
    valueResolver: cfg => cfg.imageBaseUrl
  }
];

const SETTINGS_MAP = new Map(SETTINGS_DEFINITIONS.map(def => [def.key, def]));

function buildSettingsPayload(configSnapshot = config) {
  return {
    updatedAt: new Date().toISOString(),
    groups: buildSettingsSummary(configSnapshot)
  };
}

// 为了防止误配置导致管理面板完全裸露，这里强制要求配置 PANEL_PASSWORD
if (!config.panelPassword) {
  logger.error(
    'PANEL_PASSWORD 环境变量未配置，出于安全考虑服务将不会启动，请在 Docker 环境变量中设置 PANEL_PASSWORD。'
  );
  process.exit(1);
}

// 启动时校验必须存在的环境变量，防止无认证暴露
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

const PANEL_AUTH_ENABLED = isPanelPasswordConfigured();
// 使用内存 Map 保存会话：token -> 过期时间戳
const panelSessions = new Map();

// ===== Helper functions for OpenAI-compatible responses =====

const createResponseMeta = () => ({
  id: `chatcmpl-${Date.now()}`,
  created: Math.floor(Date.now() / 1000)
});

// Calculate retry delay: respect retry-after header, or use exponential backoff with jitter
const calculateRetryDelay = (attempt, error) => {
  const initialDelay = 1000;
  const maxDelay = 10000;

  // 1. Check retry-after from error object (already parsed by client.js)
  if (error?.retryAfter && typeof error.retryAfter === 'number') {
    return error.retryAfter; // Already in milliseconds
  }

  // 2. Check retry-after header directly
  const retryAfter = error?.response?.headers?.['retry-after'] || error?.headers?.['retry-after'];
  if (retryAfter) {
    const delay = parseInt(retryAfter, 10);
    if (!isNaN(delay)) return delay * 1000; // seconds to ms
  }

  // 3. Fallback: Exponential backoff with jitter
  const backoff = Math.min(initialDelay * Math.pow(2, attempt), maxDelay);
  const jitter = Math.random() * 1000;
  return backoff + jitter;
};

const setStreamHeaders = res => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  // Heartbeat: send SSE comment every 15s to keep connection alive
  if (!res.locals) res.locals = {};
  if (!res.locals.heartbeatTimer) {
    res.locals.heartbeatTimer = setInterval(() => {
      if (!res.writableEnded && res.headersSent) {
        res.write(': keep-alive\n\n');
      }
    }, 15000);

    // Ensure timer is cleared if request closes unexpectedly
    res.on('close', () => {
      if (res.locals?.heartbeatTimer) {
        clearInterval(res.locals.heartbeatTimer);
        res.locals.heartbeatTimer = null;
      }
    });
  }
};

const createStreamChunk = (id, created, model, delta, finish_reason = null, usage = null) => ({
  id,
  object: 'chat.completion.chunk',
  created,
  model,
  choices: [{ index: 0, delta, finish_reason }],
  ...(usage ? { usage } : {})
});

const writeStreamData = (res, data) => {
  res.write(`data: ${JSON.stringify(data)}\n\n`);
};

const endStream = (res, id, created, model, finish_reason, usage = null) => {
  // Clear heartbeat timer before ending stream
  if (res.locals?.heartbeatTimer) {
    clearInterval(res.locals.heartbeatTimer);
    res.locals.heartbeatTimer = null;
  }
  writeStreamData(res, createStreamChunk(id, created, model, {}, finish_reason, usage));
  res.write('data: [DONE]\n\n');
  res.end();
};

// ===== Global middleware =====

app.use(express.json({ limit: config.security.maxRequestSize }));
app.use(express.urlencoded({ extended: false }));

// Static images for generated image URLs
app.use('/images', express.static(path.join(__dirname, '../../public/images')));

// Request body size error handler
app.use((err, req, res, next) => {
  if (err && err.type === 'entity.too.large') {
    return res
      .status(413)
      .json({ error: `Request entity too large, max ${config.security.maxRequestSize}` });
  }
  return next(err);
});

// Basic request logging (skip images / favicon)
app.use((req, res, next) => {
  if (!req.path.startsWith('/images') && !req.path.startsWith('/favicon.ico')) {
    const start = Date.now();
    res.on('finish', () => {
      const clientIP = req.headers['x-forwarded-for'] ||
        req.headers['x-real-ip'] ||
        req.connection?.remoteAddress ||
        req.socket?.remoteAddress ||
        req.ip ||
        'unknown';
      const userAgent = req.headers['user-agent'] || '';
      logger.request(req.method, req.path, res.statusCode, Date.now() - start, clientIP, userAgent);
    });
  }
  next();
});

// 根路径：未登录时跳转登录页，已登录则进入管理面板
app.get('/', (req, res) => {
  if (isPanelAuthed(req)) {
    return res.redirect('/admin/oauth');
  }
  return res.redirect('/admin/login');
});

// API key check for /v1/* 以及 /{credential}/v1/* endpoints（API_KEY 在启动时强制要求配置）
const isProtectedApiPath = pathname => {
  const normalized = pathname || '';
  return /^\/(?:[\w-]+\/)?v1\//.test(normalized);
};

function extractApiKeyFromHeaders(req) {
  const headers = req.headers || {};
  const authHeader = headers.authorization;
  if (authHeader?.startsWith('Bearer ')) return authHeader.slice(7);
  if (authHeader) return authHeader;
  // 兼容各种大小写/横线/下划线写法
  const candidates = [
    headers['x-api-key'],
    headers['api-key'],
    headers['x-api_key'],
    headers['api_key']
  ];
  return candidates.find(v => v) || null;
}

function validateApiKey(req) {
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

function requireApiKey(req, res, next) {
  const result = validateApiKey(req);
  if (!result.ok) {
    logger.warn(`API Key 鉴权失败: ${req.method} ${req.originalUrl || req.url}`);
    return res.status(result.status).json({ error: result.message });
  }
  return next();
}

app.use((req, res, next) => {
  if (isProtectedApiPath(req.path)) {
    const result = validateApiKey(req);
    if (!result.ok) {
      logger.warn(`API Key 鉴权失败: ${req.method} ${req.path}`);
      return res.status(result.status).json({ error: result.message });
    }
  }
  next();
});

// 简单健康检查接口，用于 Docker / 监控探活
app.get('/healthz', (req, res) => {
  const now = new Date();
  const serverTime = now.toISOString();
  const deltaMinutes = 8 * 60 + now.getTimezoneOffset();
  const chinaDate = new Date(now.getTime() + deltaMinutes * 60000);
  const chinaTime = chinaDate.toISOString();

  res.json({
    status: 'ok',
    uptime: process.uptime(),
    serverTime,
    chinaTime
  });
});

// ===== OAuth + simple admin panel =====

function getSessionTokenFromReq(req) {
  const cookie = req.headers.cookie;
  if (!cookie) return null;
  const item = cookie
    .split(';')
    .map(s => s.trim())
    .find(c => c.startsWith('panel_session='));
  if (!item) return null;
  return decodeURIComponent(item.slice('panel_session='.length));
}

function isPanelAuthed(req) {
  if (!PANEL_AUTH_ENABLED) return true;
  const token = getSessionTokenFromReq(req);
  if (!token) return false;

  const expiresAt = panelSessions.get(token);
  if (!expiresAt) return false;

  // 超过有效期自动失效并清理
  if (Date.now() > expiresAt) {
    panelSessions.delete(token);
    return false;
  }

  return true;
}

function requirePanelAuthPage(req, res, next) {
  if (!isPanelPasswordConfigured()) return next();
  if (isPanelAuthed(req)) return next();
  return res.redirect('/admin/login');
}

function requirePanelAuthApi(req, res, next) {
  if (!isPanelPasswordConfigured()) return next();
  if (isPanelAuthed(req)) return next();
  return res.status(401).json({ error: 'Unauthorized' });
}

function readAccountsSafe() {
  const usageMap = getUsageSummary();
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) return [];
    const raw = fs.readFileSync(ACCOUNTS_FILE, 'utf-8');
    const data = JSON.parse(raw);
    if (!Array.isArray(data)) return [];
    return data.map((acc, index) => ({
      index,
      projectId: acc.projectId || null,
      email: acc.email || acc.user_email || acc.userEmail || null,
      enable: acc.enable !== false,
      hasRefreshToken: !!acc.refresh_token,
      createdAt: acc.timestamp || null,
      expiresIn: acc.expires_in || null,
      usage: usageMap[acc.projectId] || {
        total: 0,
        success: 0,
        failed: 0,
        lastUsedAt: null,
        models: []
      }
    }));
  } catch (e) {
    logger.error(`读取 accounts.json 失败: ${e.message}`);
    return [];
  }
}

function parseTimestamp(raw) {
  if (raw && Number.isFinite(Number(raw.timestamp))) {
    return Number(raw.timestamp);
  }

  const dateString = raw?.created_at || raw?.createdAt;
  if (dateString) {
    const parsed = Date.parse(dateString);
    if (!Number.isNaN(parsed)) return parsed;
  }

  return Date.now();
}

function normalizeTomlAccount(raw, { filterDisabled = false } = {}) {
  if (!raw || typeof raw !== 'object') return null;

  const accessToken = raw.access_token ?? raw.accessToken;
  const refreshToken = raw.refresh_token ?? raw.refreshToken;

  const isDisabled = raw.disabled === true || raw.enable === false;
  if (filterDisabled && isDisabled) return null;

  if (!accessToken || !refreshToken) return null;

  const normalized = {
    access_token: accessToken,
    refresh_token: refreshToken,
    expires_in: Number.isFinite(Number(raw.expires_in ?? raw.expiresIn))
      ? Number(raw.expires_in ?? raw.expiresIn)
      : 3600,
    timestamp: parseTimestamp(raw),
    enable: !isDisabled
  };

  const projectId = raw.projectId ?? raw.project_id;
  if (projectId) normalized.projectId = projectId;

  const copyPairs = [
    ['email', 'email'],
    ['user_id', 'user_id'],
    ['userId', 'user_id'],
    ['user_email', 'user_email'],
    ['userEmail', 'user_email'],
    ['last_used', 'last_used'],
    ['lastUsed', 'last_used'],
    ['created_at', 'created_at'],
    ['createdAt', 'created_at'],
    ['next_reset_time', 'next_reset_time'],
    ['nextResetTime', 'next_reset_time'],
    ['daily_limit_claude', 'daily_limit_claude'],
    ['dailyLimitClaude', 'daily_limit_claude'],
    ['daily_limit_gemini', 'daily_limit_gemini'],
    ['dailyLimitGemini', 'daily_limit_gemini'],
    ['daily_limit_total', 'daily_limit_total'],
    ['dailyLimitTotal', 'daily_limit_total'],
    ['claude_sonnet_4_5_calls', 'claude_sonnet_4_5_calls'],
    ['gemini_3_pro_calls', 'gemini_3_pro_calls'],
    ['total_calls', 'total_calls'],
    ['last_success', 'last_success'],
    ['error_codes', 'error_codes'],
    ['gemini_3_series_banned_until', 'gemini_3_series_banned_until']
  ];

  for (const [source, target] of copyPairs) {
    if (raw[source] !== undefined) {
      normalized[target] = raw[source];
    }
  }

  return normalized;
}

function mergeAccounts(existing, incoming, replaceExisting = false) {
  if (replaceExisting) return incoming;

  const map = new Map();

  existing.forEach((acc, idx) => {
    const key = acc.refresh_token || acc.access_token || `existing-${idx}`;
    map.set(key, acc);
  });

  incoming.forEach((acc, idx) => {
    const key = acc.refresh_token || acc.access_token || `incoming-${idx}`;
    const current = map.get(key) || {};
    map.set(key, { ...current, ...acc });
  });

  return Array.from(map.values());
}

// Simple login page for admin panel
app.get('/admin/login', (req, res) => {
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
});

app.post('/admin/login', (req, res) => {
  if (!PANEL_AUTH_ENABLED) {
    return res.redirect('/admin/oauth');
  }

  const { username, password } = req.body || {};
  if (username === getPanelUser() && password === config.panelPassword) {
    const token = crypto.randomBytes(24).toString('hex');
    const expiresAt = Date.now() + PANEL_SESSION_TTL_MS;
    panelSessions.set(token, expiresAt);
    res.setHeader(
      'Set-Cookie',
      `panel_session=${encodeURIComponent(
        token
      )}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${Math.floor(
        PANEL_SESSION_TTL_MS / 1000
      )}`
    );
    return res.redirect('/admin/oauth');
  }

  return res
    .status(401)
    .send('<h1>登录失败</h1><p>用户名或密码错误。</p><p><a href="/admin/login">返回重试</a></p>');
});

// Logout endpoint for admin panel
app.post('/admin/logout', (req, res) => {
  const token = getSessionTokenFromReq(req);
  if (token) {
    panelSessions.delete(token);
  }

  res.setHeader(
    'Set-Cookie',
    'panel_session=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0'
  );

  if (req.accepts('json')) {
    return res.json({ success: true });
  }

  return res.redirect('/admin/login');
});

// Return Google OAuth URL as JSON for front-end
// 前端现在采用“手动粘贴回调 URL”模式，这里仍然返回带 redirect_uri 的完整授权链接
app.get('/auth/oauth/url', requirePanelAuthApi, (req, res) => {
  const redirectUri = `http://localhost:${config.server.port}/oauth-callback`;

  const url = buildAuthUrl(redirectUri, OAUTH_STATE);
  res.json({ url });
});

// 仅作为提示页面使用：不再在这里直接交换 token
// 用户在完成授权后，需要复制浏览器地址栏中的完整 URL，回到管理面板粘贴，由新的解析接口处理
app.get(['/oauth-callback', '/auth/oauth/callback'], (req, res) => {
  return res.send(
    '<!DOCTYPE html>' +
    '<html lang="zh-CN"><head><meta charset="utf-8" />' +
    '<title>授权回调 - 请复制地址栏 URL</title>' +
    '<style>body{font-family:system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;background:#f9fafb;margin:0;padding:24px;color:#111827;}h1{font-size:20px;margin:0 0 12px;}p{margin:6px 0;}code{padding:2px 4px;background:#e5e7eb;border-radius:4px;}</style>' +
    '</head><body>' +
    '<h1>授权流程已返回回调地址</h1>' +
    '<p>请复制当前页面浏览器地址栏中的完整 URL，回到 <code>Antigravity</code> 管理面板，在“粘贴回调 URL”输入框中粘贴并提交。</p>' +
    '<p>提交后，服务端会解析 URL 中的 <code>code</code> 参数并完成账户添加。</p>' +
    '</body></html>'
  );
});

// 解析用户粘贴的回调 URL，交换 code 为 token，写入 accounts.json 并刷新 TokenManager
app.post('/auth/oauth/parse-url', requirePanelAuthApi, async (req, res) => {
  const { url, replaceIndex, customProjectId, allowRandomProjectId } = req.body || {};

  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'url 字段必填且必须为字符串' });
  }

  let parsed;
  try {
    parsed = new URL(url);
  } catch (e) {
    return res.status(400).json({ error: '无效的 URL，无法解析' });
  }

  const code = parsed.searchParams.get('code');
  const state = parsed.searchParams.get('state');

  if (!code) {
    return res.status(400).json({ error: 'URL 中缺少 code 参数' });
  }

  if (state && state !== OAUTH_STATE) {
    logger.warn('OAuth state mismatch in pasted URL, possible CSRF or wrong URL.');
    return res.status(400).json({ error: 'state 校验失败，请确认粘贴的是最新的授权回调地址' });
  }

  // 直接使用构造OAuth链接时相同的 redirectUri，避免不匹配问题
  const redirectUri = `http://localhost:${config.server.port}/oauth-callback`;

  try {
    const tokenData = await exchangeCodeForToken(code, redirectUri);

    let projectId = null;
    let userEmail = null;
    let projectResolveError = null;

    // 优先使用用户自定义的项目ID
    if (customProjectId && typeof customProjectId === 'string' && customProjectId.trim()) {
      projectId = customProjectId.trim();
      logger.info(`使用用户自定义项目ID: ${projectId}`);
    } else if (tokenData?.access_token) {
      // 自动获取项目ID的逻辑
      try {
        // 获取用户邮箱
        userEmail = await fetchUserEmail(tokenData.access_token);
        logger.info(`成功获取用户邮箱: ${userEmail}`);

        // 使用更可靠的Resource Manager方法获取项目ID
        const result = await resolveProjectIdFromAccessToken(tokenData.access_token);
        if (result.projectId) {
          projectId = result.projectId;
          logger.info(`通过Resource Manager获取到项目ID: ${projectId}`);
        } else {
          // 备用方案：使用原有的loadCodeAssist方法
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

    // 如果无法获取项目ID，尝试使用备用方案
    if (!projectId && !allowRandomProjectId) {
      const message =
        projectResolveError?.message ||
        '无法自动获取 Google 项目 ID，对应接口的访问可能出现 403 错误，请检查权限和 API 组件，或选择使用随机 projectId 再申请！';
      return res.status(400).json({ error: message, code: 'PROJECT_ID_MISSING' });
    }

    if (!projectId && allowRandomProjectId) {
      projectId = generateProjectId();
      logger.info(`使用随机生成的项目ID: ${projectId}`);
    }

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

    let accounts = [];
    try {
      if (fs.existsSync(ACCOUNTS_FILE)) {
        accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
      }
    } catch {
      logger.warn('Failed to read accounts.json, will create new file');
    }

    if (!Array.isArray(accounts)) accounts = [];
    if (Number.isInteger(replaceIndex) && replaceIndex >= 0 && replaceIndex < accounts.length) {
      accounts[replaceIndex] = account;
    } else {
      accounts.push(account);
    }

    const dir = path.dirname(ACCOUNTS_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');

    // Reload TokenManager so new account becomes usable without restart
    if (typeof tokenManager.initialize === 'function') {
      tokenManager.initialize();
    }

    logger.info(`Token 已保存到 ${ACCOUNTS_FILE}`);

    return res.json({ success: true });
  } catch (e) {
    logger.error('OAuth 交换 token 失败:', e.message);
    return res.status(500).json({ error: `交换 token 失败: ${e.message}` });
  }
});

// Import accounts from TOML and merge into accounts.json
app.post('/auth/accounts/import-toml', requirePanelAuthApi, (req, res) => {
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

  let existing = [];
  if (!replaceExisting && fs.existsSync(ACCOUNTS_FILE)) {
    try {
      existing = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
      if (!Array.isArray(existing)) existing = [];
    } catch (e) {
      logger.warn(`读取 accounts.json 失败，将忽略已有账号: ${e.message}`);
      existing = [];
    }
  }

  const merged = mergeAccounts(existing, normalized, replaceExisting);

  const dir = path.dirname(ACCOUNTS_FILE);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }

  fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(merged, null, 2), 'utf-8');

  if (typeof tokenManager.initialize === 'function') {
    tokenManager.initialize();
  }

  return res.json({
    success: true,
    imported: normalized.length,
    skipped,
    total: merged.length
  });
});

// Simple JSON list of accounts for front-end
app.get('/auth/accounts', requirePanelAuthApi, (req, res) => {
  res.json({ accounts: readAccountsSafe() });
});

// Refresh all accounts
app.post('/auth/accounts/refresh-all', requirePanelAuthApi, async (req, res) => {
  try {
    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    if (!Array.isArray(accounts) || accounts.length === 0) {
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
        accounts[i] = account;
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

    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');
    tokenManager.initialize();

    res.json({ success: true, refreshed, failed, total: accounts.length, results });
  } catch (e) {
    logger.error('批量刷新凭证失败', e.message);
    res.status(500).json({ error: e.message || '批量刷新失败' });
  }
});

// Get credential freeze history
app.get('/auth/accounts/freeze-history', requirePanelAuthApi, (req, res) => {
  res.json({ history: tokenManager.getFreezeHistory() });
});

// Manually refresh a single account by index
app.post('/auth/accounts/:index/refresh', requirePanelAuthApi, async (req, res) => {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) return res.status(400).json({ error: '无效的账号序号' });

  try {
    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    const target = accounts[index];
    if (!target) return res.status(404).json({ error: '账号不存在' });
    await tokenManager.refreshToken(target);
    accounts[index] = target;
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('刷新账号失败', e.message);
    res.status(500).json({ error: e.message || '刷新失败' });
  }
});

app.post('/auth/accounts/:index/refresh-project-id', requirePanelAuthApi, async (req, res) => {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) return res.status(400).json({ error: 'invalid account index' });

  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) {
      return res.status(404).json({ error: 'accounts.json not found' });
    }

    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    const target = accounts[index];
    if (!target) return res.status(404).json({ error: 'account not found' });

    let accessToken = target.access_token;

    if (!accessToken && target.refresh_token) {
      try {
        await tokenManager.refreshToken(target);
        accessToken = target.access_token;
      } catch (err) {
        logger.error('failed to refresh token before resolving project id', err.message);
        return res
          .status(500)
          .json({ error: err?.message || 'failed to refresh token for this account' });
      }
    }

    if (!accessToken) {
      return res
        .status(400)
        .json({ error: 'no usable access token for this account' });
    }

    const result = await resolveProjectIdFromAccessToken(accessToken);
    if (!result.projectId) {
      const errorMessage =
        result.error?.message ||
        'failed to resolve project id from Resource Manager';
      logger.warn(
        'refresh project id failed: unable to resolve project id from Resource Manager',
        errorMessage
      );
      return res.status(500).json({ error: errorMessage });
    }

    target.projectId = result.projectId;
    accounts[index] = target;

    const dir = path.dirname(ACCOUNTS_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');

    if (typeof tokenManager.initialize === 'function') {
      tokenManager.initialize();
    }

    return res.json({ success: true, projectId: result.projectId });
  } catch (e) {
    logger.error('refresh project id failed', e.message);
    return res.status(500).json({ error: e.message || 'refresh project id failed' });
  }
});

// Delete an account
app.delete('/auth/accounts/:index', requirePanelAuthApi, (req, res) => {
  const index = Number.parseInt(req.params.index, 10);
  if (Number.isNaN(index)) return res.status(400).json({ error: '无效的账号序号' });

  try {
    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    if (!accounts[index]) return res.status(404).json({ error: '账号不存在' });
    accounts.splice(index, 1);
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('删除账号失败', e.message);
    res.status(500).json({ error: e.message || '删除失败' });
  }
});

// Toggle enable/disable for an account
app.post('/auth/accounts/:index/enable', requirePanelAuthApi, (req, res) => {
  const index = Number.parseInt(req.params.index, 10);
  const { enable = true } = req.body || {};
  if (Number.isNaN(index)) return res.status(400).json({ error: '无效的账号序号' });

  try {
    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    if (!accounts[index]) return res.status(404).json({ error: '账号不存在' });
    accounts[index].enable = !!enable;
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');
    tokenManager.initialize();
    res.json({ success: true });
  } catch (e) {
    logger.error('更新账号状态失败', e.message);
    res.status(500).json({ error: e.message || '更新失败' });
  }
});

app.get('/admin/settings', requirePanelAuthApi, (req, res) => {
  res.json(buildSettingsPayload());
});

app.post('/admin/settings', requirePanelAuthApi, (req, res) => {
  const { key, value } = req.body || {};

  if (!key || typeof key !== 'string') {
    return res.status(400).json({ error: '缺少 key，无法更新配置' });
  }

  if (!SETTINGS_MAP.has(key)) {
    return res.status(400).json({ error: `不支持修改的配置项: ${key}` });
  }

  // 检查是否为Docker专用配置
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
});

app.get('/admin/panel-config', requirePanelAuthApi, (req, res) => {
  res.json({ apiKey: config.security.apiKey || null });
});

app.get('/admin/logs/usage', requirePanelAuthApi, (req, res) => {
  const windowMinutes = 60;
  const limitPerCredential = Number.isFinite(Number(tokenManager.hourlyLimit))
    ? Number(tokenManager.hourlyLimit)
    : null;
  const usage = getUsageCountsWithinWindow(windowMinutes * 60 * 1000);

  res.json({ windowMinutes, limitPerCredential, usage, updatedAt: new Date().toISOString() });
});

// 调用日志配置：仅影响管理面板里的调用日志存储，不影响终端控制台输出
app.get('/admin/logs/settings', requirePanelAuthApi, (req, res) => {
  const raw = (config.logging.requestLogLevel || '').toLowerCase();
  const level = ['off', 'error', 'all'].includes(raw) ? raw : 'all';

  const maxItems = config.logging.requestLogMaxItems;
  const retentionDays = config.logging.requestLogRetentionDays;

  res.json({
    level,
    maxItems,
    retentionDays
  });
});

app.post('/admin/logs/settings', requirePanelAuthApi, (req, res) => {
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
});

// Recent request logs
app.get('/admin/logs', requirePanelAuthApi, (req, res) => {
  const limit = req.query.limit ? Number.parseInt(req.query.limit, 10) : 200;
  res.json({ logs: getRecentLogs(limit) });
});

app.post('/admin/logs/clear', requirePanelAuthApi, (req, res) => {
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
});

app.get('/admin/logs/:id', requirePanelAuthApi, (req, res) => {
  const detail = getLogDetail(req.params.id);
  if (!detail) return res.status(404).json({ error: '日志不存在或已过期' });
  res.json({ log: detail });
});

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

// API Key 鉴权的额度查询接口
app.get('/admin/quota/list', requireApiKey, (req, res) => {
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) {
      return res.json({ code: '成功为200', msg: '成功就写获取成功', enabled: 0 });
    }

    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    const enabled = Array.isArray(accounts)
      ? accounts.filter(acc => acc && acc.enable !== false).length
      : 0;

    return res.json({ code: '成功为200', msg: '成功就写获取成功', enabled });
  } catch (e) {
    logger.error('/admin/quota/list 获取启用凭证数量失败:', e.message);
    return res
      .status(500)
      .json({ error: e.message || '获取启用凭证数量失败' });
  }
});

app.get('/admin/quota/all', requireApiKey, async (req, res) => {
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) {
      return res.status(404).json({ error: 'accounts.json 不存在' });
    }

    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    if (!Array.isArray(accounts) || accounts.length === 0) {
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
});

// 获取 Token 运行时统计
app.get('/admin/tokens/stats', requirePanelAuthApi, (req, res) => {
  try {
    const stats = {};
    if (tokenManager && Array.isArray(tokenManager.tokens)) {
      tokenManager.tokens.forEach((token) => {
        // 使用 projectId 作为 key，与前端 accountsData 匹配
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
});

// 额度查询接口
app.get('/admin/tokens/:index/quotas', requirePanelAuthApi, async (req, res) => {
  try {
    const index = Number.parseInt(req.params.index, 10);
    if (Number.isNaN(index)) {
      return res.status(400).json({ error: '无效的凭证序号' });
    }

    const accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf-8'));
    const target = accounts[index];
    if (!target) {
      return res.status(404).json({ error: '凭证不存在' });
    }

    if (!target.refresh_token) {
      return res.status(400).json({ error: '凭证缺少refresh_token' });
    }

    // 使用refreshToken作为缓存键
    const quotas = await quotaManager.getQuotas(target.refresh_token, target);

    // 禁止浏览器缓存额度结果，确保每次查询直连谷歌
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');

    res.json({ success: true, data: quotas });
  } catch (e) {
    logger.error('获取额度失败:', e.message);
    res.status(500).json({ error: e.message || '获取额度失败' });
  }
});

// Minimal HTML admin panel for OAuth (served as static file)
app.get('/admin/oauth', requirePanelAuthPage, (req, res) => {
  const filePath = path.join(__dirname, '..', '..', 'public', 'admin', 'index.html');
  res.sendFile(filePath);
});

// 将 Gemini 兼容响应中的 inlineData 落地为 URL，避免下游自行处理 base64
function attachImageUrlsToGeminiResponse(response) {
  if (!response?.candidates) return response;
  try {
    for (const candidate of response.candidates) {
      const parts = candidate?.content?.parts;
      if (!Array.isArray(parts)) continue;
      for (const part of parts) {
        const inline = part?.inlineData || part?.inline_data;
        if (!inline || typeof inline.data !== 'string' || !inline.data.trim()) continue;
        const mimeType = inline.mimeType || inline.mime_type || 'image/png';
        const url = saveBase64Image(inline.data, mimeType);
        if (part.inlineData) {
          part.inlineData.url = url;
        }
        if (part.inline_data) {
          part.inline_data.url = url;
        }
        // 额外放一份 imageUrl 便于客户端直接取用
        part.imageUrl = url;
      }
    }
  } catch (err) {
    logger.warn('处理 Gemini 响应图片为 URL 时出错:', err.message);
  }
  return response;
}

// Static assets for admin panel
const adminStatic = express.static(path.join(__dirname, '..', '..', 'public', 'admin'), {
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

app.use('/admin', (req, res, next) => {
  if (req.method === 'GET' && publicAdminAssets.has(req.path)) {
    return adminStatic(req, res, next);
  }

  // 复用页面级的鉴权逻辑，未登录则重定向到 /admin/login
  requirePanelAuthPage(req, res, err => {
    if (err) return next(err);
    return adminStatic(req, res, next);
  });
});

// ===== API routes =====

const createChatCompletionHandler = (resolveToken, options = {}) => async (req, res) => {
  const { messages, model, stream = true, tools, ...params } = req.body || {};
  const startedAt = Date.now();
  const correlationId = req.headers['x-correlation-id'] || req.headers['x-request-id'] || crypto.randomUUID();
  const requestSnapshot = createRequestSnapshot(req);
  let streamEventsForLog = [];
  let responseBodyForLog = null;
  let responseSummaryForLog = null;

  let token = null;
  const writeLog = ({ success, status, message, isRetry = false, retryCount = 0, willRetry = false, errorPreview = null, rawResponse = null }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model: model || req.body?.model || 'unknown',
      projectId: token?.projectId || null,
      success,
      status,
      message,
      correlationId,
      isRetry,
      retryCount,
      willRetry,
      errorPreview,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog,
          rawBody: rawResponse,
          modelOutput: responseSummaryForLog
        }
      }
    });
    // 同时输出到控制台详细日志
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog,
          modelOutput: responseSummaryForLog
        },
        error: success ? undefined : logMessage
      });
    }
  };

  if (!messages) {
    res.status(400).json({ error: 'messages is required' });
    writeLog({ success: false, status: 400, message: 'messages is required' });
    return;
  }

  const maxAttempts = config.retry?.maxAttempts || 3;
  const retryStatusCodes = config.retry?.statusCodes || [429, 500];
  let attempt = 0;
  let retryCountForLog = 0; // 独立追踪实际重试次数 (Fix Issue 3: 重试日志计数)
  let lastError = null;
  const excludedTokenIds = new Set();

  // 429 重试策略状态变量
  let retryingToken = null;
  const retried429Tokens = new Set();

  while (attempt < maxAttempts) {
    attempt++;
    const isLastAttempt = attempt === maxAttempts;

    // 重置日志相关变量
    streamEventsForLog = [];
    responseBodyForLog = null;
    responseSummaryForLog = null;
    token = null;

    try {
      if (res.writableEnded || req.destroyed) break;

      // 如果有待重试的凭证，优先使用它
      if (retryingToken) {
        token = retryingToken;
        retryingToken = null;
      } else {
        token = await resolveToken(req, excludedTokenIds);
      }
      if (!token) {
        const noTokenError = new Error(
          options.tokenMissingError || '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。'
        );
        noTokenError.status = options.tokenMissingStatus || 503;
        noTokenError.code = 'NO_TOKEN';
        throw noTokenError;
      }

    // 兼容模型别名后缀 -1k/-2k/-4k：用于指定分辨率，发送给上游时去掉后缀
    let upstreamModel = model;
    let imageSizeFromModel = null;
    if (typeof model === 'string') {
      const match = model.match(/^(.*-image)(?:-(1k|2k|4k))$/i);
      if (match) {
        upstreamModel = match[1];
        imageSizeFromModel = match[2].toUpperCase(); // 1K/2K/4K
      }
    }

    // 将分辨率写入参数（仅当用户未显式传入时）
    const paramsWithImageSize = { ...params };
    const userHasImageSize =
      params.image_size ||
      params.imageSize ||
      params?.generation_config?.image_size ||
      params?.generation_config?.imageSize ||
      params?.generation_config?.image_config?.image_size ||
      params?.generation_config?.image_config?.imageSize ||
      params?.generationConfig?.image_size ||
      params?.generationConfig?.imageSize ||
      params?.generationConfig?.image_config?.image_size ||
      params?.generationConfig?.image_config?.imageSize;
    if (imageSizeFromModel && !userHasImageSize) {
      paramsWithImageSize.image_size = imageSizeFromModel;
    }

    const isImageModel = typeof upstreamModel === 'string' && upstreamModel.includes('-image');
    const requestBody = generateRequestBody(messages, upstreamModel, paramsWithImageSize, tools, token);

    if (isImageModel) {
      // 为图像模型配置思维链、响应模态，并兼容 imageConfig 等参数，使图片模型能返回图片
      const userGenerationConfig = paramsWithImageSize.generation_config || paramsWithImageSize.generationConfig || {};
      const userImageConfig =
        paramsWithImageSize.image_config ||
        paramsWithImageSize.imageConfig ||
        userGenerationConfig.image_config ||
        userGenerationConfig.imageConfig ||
        {};
      const aspectRatio =
        paramsWithImageSize.aspect_ratio ||
        paramsWithImageSize.aspectRatio ||
        userImageConfig.aspect_ratio ||
        userImageConfig.aspectRatio;
      const imageSize =
        paramsWithImageSize.image_size ||
        paramsWithImageSize.imageSize ||
        userImageConfig.image_size ||
        userImageConfig.imageSize;
      const responseModalities =
        paramsWithImageSize.response_modalities ||
        paramsWithImageSize.responseModalities ||
        userGenerationConfig.response_modalities ||
        userGenerationConfig.responseModalities;

      const mergedImageConfig = {};
      if (aspectRatio) mergedImageConfig.aspectRatio = aspectRatio;
      if (imageSize) mergedImageConfig.imageSize = imageSize;

      const mergedGenerationConfig = {
        ...requestBody.request.generationConfig,
        ...userGenerationConfig,
        responseModalities: responseModalities || ["TEXT", "IMAGE"],
        thinkingConfig: {
          includeThoughts: true,
          thinkingBudget: 1024
        },
        candidateCount: 1
      };
      if (Object.keys(mergedImageConfig).length > 0) {
        mergedGenerationConfig.imageConfig = mergedImageConfig;
      }

      requestBody.request.generationConfig = mergedGenerationConfig;
      requestBody.requestType = 'image_gen';
      requestBody.request.systemInstruction.parts[0].text +=
        '（当前作为图像生成模型使用，请根据描述生成图片）';
      delete requestBody.request.tools;
      delete requestBody.request.toolConfig;
    }

    const { id, created } = createResponseMeta();

    if (stream) {
      // Headers will be sent on first data chunk to enable retry on 429

      if (isImageModel) {
        // 图像模型使用流式API，实现思维链实时传输
        const imageUrls = [];
        const { usage } = await generateAssistantResponse(requestBody, token, data => {
          if (!res.headersSent) setStreamHeaders(res);
          streamEventsForLog.push(data);

          if (data.type === 'thinking') {
            // 思维链内容实时发送
            writeStreamData(res, createStreamChunk(id, created, model, { reasoning_content: data.content }));
          } else if (data.type === 'image') {
            // 收集图片URL，最后统一发送
            imageUrls.push(data.url);
          } else if (data.type === 'text') {
            // 文本内容
            writeStreamData(res, createStreamChunk(id, created, model, { content: data.content }));
          }
        });

        // 发送所有图片
        if (imageUrls.length > 0) {
          const markdown = imageUrls.map(url => `![image](${url})`).join('\n\n');
          writeStreamData(res, createStreamChunk(id, created, model, { content: markdown }));
        }

        if (!res.headersSent) setStreamHeaders(res);
        endStream(res, id, created, model, 'stop', usage);
        responseBodyForLog = { stream: true, image: true, usage, events: streamEventsForLog };
        responseSummaryForLog = summarizeStreamEvents(streamEventsForLog);
      } else {
        let hasToolCall = false;
        const { usage, finishReason } = await generateAssistantResponse(requestBody, token, data => {
          if (!res.headersSent) setStreamHeaders(res);
          streamEventsForLog.push(data);

          let delta = {};
          if (data.type === 'tool_calls') {
            // 为兼容 OpenAI 流式规范，这里补充 index 字段
            delta = {
              tool_calls: (data.tool_calls || []).map((toolCall, index) => ({
                index,
                id: toolCall.id,
                type: toolCall.type,
                function: toolCall.function
              }))
            };
          } else if (data.type === 'reasoning') {
            // OpenAI Responses API reasoning 格式透传
            delta = {
              type: 'reasoning',
              id: data.id,
              summary: data.summary
            };
          } else if (data.type === 'thinking') {
            // 旧版兼容：思维链内容放入 reasoning_content
            const cleanContent = data.content.replace(/^<思考>\n?|\n?<\/思考>$/g, '');
            delta = { reasoning_content: cleanContent };
          } else if (data.type === 'text') {
            // 普通文本内容放入 content（需要过滤掉思考标签）
            const cleanContent = data.content.replace(/<思考>[\s\S]*?<\/思考>/g, '');
            if (cleanContent) {
              delta = { content: cleanContent };
            }
          }

          // 只有当 delta 有内容时才发送
          if (Object.keys(delta).length > 0) {
            if (data.type === 'tool_calls') hasToolCall = true;
            writeStreamData(res, createStreamChunk(id, created, model, delta));
          }
        });
        if (!res.headersSent) setStreamHeaders(res);
        // 优先使用上游返回的 finishReason，回退逻辑保持兼容
        const finalFinishReason = finishReason || (hasToolCall ? 'tool_calls' : 'stop');
        endStream(res, id, created, model, finalFinishReason, usage);
        responseBodyForLog = { stream: true, events: streamEventsForLog, usage };
        responseSummaryForLog = summarizeStreamEvents(streamEventsForLog);
      }
    } else {
      const { content, toolCalls, usage, finishReason } = await generateAssistantResponseNoStream(
        requestBody,
        token
      );
      const message = { role: 'assistant', content };
      if (toolCalls.length > 0) message.tool_calls = toolCalls;

      // 优先使用上游返回的 finishReason
      const finalFinishReason = finishReason || (toolCalls.length > 0 ? 'tool_calls' : 'stop');

      res.json({
        id,
        object: 'chat.completion',
        created,
        model,
        choices: [
          {
            index: 0,
            message,
            finish_reason: finalFinishReason
          }
        ],
        usage: usage || null
      });
      responseBodyForLog = { stream: false, choices: [{ message, finish_reason: finalFinishReason }], usage };
      responseSummaryForLog = { text: content, tool_calls: toolCalls, usage };
    }

    // 成功：记录统计并退出
    tokenManager.recordSuccess(token);
    writeLog({
      success: true,
      status: res.statusCode || 200,
      isRetry: retryCountForLog > 0,
      retryCount: retryCountForLog
    });
    return;

    } catch (error) {
      lastError = error;
      const errorStatus = error.status || error.statusCode || error.response?.status || 500;
      // Fix: Convert errorStatus to integer early for consistent comparisons
      const errorStatusInt = parseInt(String(errorStatus), 10);
      const rawResponse = error.rawResponse || null;
      // 截取前 500 字符作为预览，方便在列表页直接查看
      const errorPreview = rawResponse
        ? (typeof rawResponse === 'string' ? rawResponse : JSON.stringify(rawResponse)).slice(0, 500)
        : null;

      // 429 重试策略：遇到 429 先等待后重试一次当前凭证，再次失败才冻结
      if (token && errorStatusInt === 429) {
        const tokenKey = tokenManager.getTokenKey(token);
        if (!retried429Tokens.has(tokenKey)) {
          const delay = calculateRetryDelay(attempt, error);
          logger.warn(`凭证 ${tokenKey} 遇到 429，等待 ${Math.round(delay)}ms 后重试当前凭证...`);

          // 记录 429 日志（标记为将要重试）
          writeLog({
            success: false,
            status: 429,
            message: `429 限流，等待 ${Math.round(delay)}ms 后重试当前凭证`,
            isRetry: retryCountForLog > 0,
            retryCount: retryCountForLog,
            willRetry: true,
            errorPreview,
            rawResponse
          });
          await new Promise(resolve => setTimeout(resolve, delay));

          retried429Tokens.add(tokenKey);
          retryingToken = token;
          attempt--; // 本次重试不计入总尝试次数
          retryCountForLog++; // 但计入实际重试计数
          continue;
        }
      }

      // 记录失败统计
      if (token) {
        tokenManager.recordFailure(token, errorStatus);
        excludedTokenIds.add(tokenManager.getTokenKey(token));
      }

      // 如果是 NO_TOKEN 错误，无法重试
      if (error.code === 'NO_TOKEN') {
        writeLog({ success: false, status: errorStatus, message: error.message, errorPreview });
        if (!res.headersSent) {
          res.status(errorStatus).json({ error: error.message });
        }
        return;
      }

      // 判断是否可重试 (errorStatusInt already defined above)
      const isRetryable = retryStatusCodes.includes(errorStatusInt) ||
        error.code === 'TOKEN_DISABLED' ||
        error.code === 'RATE_LIMITED';

      if (!isLastAttempt && isRetryable) {
        // 记录本次失败日志（标记为将要重试）
        logger.warn(`请求失败 (尝试 ${attempt}/${maxAttempts})，正在切换凭证重试: ${error.message}`);
        writeLog({
          success: false,
          status: errorStatus,
          message: error.message,
          isRetry: retryCountForLog > 0,  // 第一次尝试不是重试
          retryCount: retryCountForLog,
          willRetry: true,
          errorPreview,
          rawResponse
        });
        retryCountForLog++;
        continue;
      }

      // 最后一次尝试或不可重试
      logger.error('生成响应失败:', error.message);
      responseBodyForLog = responseBodyForLog || { error: error.message };
      writeLog({
        success: false,
        status: errorStatus,
        message: error.message,
        isRetry: retryCountForLog > 0,
        retryCount: retryCountForLog,
        errorPreview,
        rawResponse
      });

      if (!res.headersSent) {
        const { id, created } = createResponseMeta();

        // 构建更详细的错误消息
        let errorContent = `错误: ${error.message}`;
        if (retryCountForLog > 0) {
          errorContent = `请求失败 (已重试 ${retryCountForLog} 次): ${error.message}`;
        }
        if (error.code === 'RATE_LIMITED' && error.retryAfter) {
          const retrySeconds = Math.ceil(error.retryAfter / 1000);
          errorContent = `请求被限流，请等待 ${retrySeconds} 秒后重试。`;
        } else if (error.code === 'TOKEN_DISABLED') {
          errorContent = `凭证已失效或无权限，已自动切换。请重试。`;
        }

        if (stream) {
          setStreamHeaders(res);
          writeStreamData(
            res,
            createStreamChunk(id, created, model || 'unknown', { content: errorContent })
          );
          endStream(res, id, created, model || 'unknown', 'stop');
        } else {
          res.status(errorStatus).json({
            id,
            object: 'chat.completion',
            created,
            model: model || 'unknown',
            choices: [
              {
                index: 0,
                message: { role: 'assistant', content: errorContent },
                finish_reason: 'stop'
              }
            ],
            error: {
              code: error.code || 'UNKNOWN_ERROR',
              message: error.message,
              retry_after: error.retryAfter ? Math.ceil(error.retryAfter / 1000) : undefined
            }
          });
        }
      }
      return;
    }
  }
};

app.get('/v1/models', async (req, res) => {
  try {
    const models = await getAvailableModels();
    res.json(models);
  } catch (error) {
    logger.error('获取模型列表失败:', error.message);
    const clientIP = req.headers['x-forwarded-for'] ||
      req.headers['x-real-ip'] ||
      req.connection?.remoteAddress ||
      req.socket?.remoteAddress ||
      req.ip ||
      'unknown';
    const userAgent = req.headers['user-agent'] || '';
    logger.error(`/v1/models 错误详情 [${clientIP}] ${userAgent}:`, error.message);
    res.status(500).json({ error: error.message });
  }
});

app.get('/v1/lits', (req, res) => {
  const limitPerCredential = Number.isFinite(Number(tokenManager.hourlyLimit))
    ? Number(tokenManager.hourlyLimit)
    : null;
  const usageMap = new Map(
    getUsageCountsWithinWindow(60 * 60 * 1000).map(item => [item.projectId, item.count])
  );

  const credentials = (tokenManager.tokens || [])
    .filter(token => token.enable !== false)
    .map(token => {
      const used = usageMap.get(token.projectId) || 0;
      const remaining = limitPerCredential === null ? null : Math.max(limitPerCredential - used, 0);
      return {
        name: token.projectId,
        used_per_hour: used,
        remaining_per_hour: remaining
      };
    });

  res.json({
    credentials,
    windowMinutes: 60,
    limitPerCredential,
    updatedAt: new Date().toISOString()
  });
});

// Gemini 兼容接口：非流式 GenerateContent，直接接收 Gemini Request 并通过 AntigravityRequester 调用后端
const handleGeminiGenerateContent = async (req, res) => {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const model = req.params.model || req.body?.model || 'unknown';

  // 兼容模型别名后缀 -1k/-2k/-4k：用于指定分辨率，发送给上游时去掉后缀
  let upstreamModel = model;
  let imageSizeFromModel = null;
  if (typeof model === 'string') {
    const match = model.match(/^(.*-image)(?:-(1k|2k|4k))$/i);
    if (match) {
      upstreamModel = match[1];
      imageSizeFromModel = match[2].toUpperCase(); // 1K/2K/4K
    }
  }

  let token = null;
  let responseBodyForLog = null;

  const writeLog = ({ success, status, message }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model,
      projectId: token?.projectId || null,
      success,
      status,
      message,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        }
      }
    });
    // 同时输出到控制台详细日志
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        },
        error: success ? undefined : message
      });
    }
  };

  try {
    const body = req.body || {};
    // 若通过模型后缀指定分辨率且请求未显式携带，则补全到 generationConfig.imageConfig.imageSize
    if (imageSizeFromModel) {
      const genCfg = body.generationConfig || {};
      const imgCfg = genCfg.imageConfig || {};
      const hasImageSize = imgCfg.imageSize || imgCfg.image_size;
      if (!hasImageSize) {
        imgCfg.imageSize = imageSizeFromModel;
        genCfg.imageConfig = imgCfg;
        body.generationConfig = genCfg;
      }
    }
    if (!Array.isArray(body.contents) || body.contents.length === 0) {
      const status = 400;
      const message = 'contents is required for Gemini generateContent';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    // 将 Gemini 原生请求包装成 Antigravity 请求体
    const requestBody = generateRequestBodyFromGemini(body, upstreamModel, token);

    // 当前只支持非流式：即官方 Gemini 的 :generateContent 语义
    const geminiResponse = await generateGeminiResponseNoStream(requestBody, token);
    const responseWithUrls = attachImageUrlsToGeminiResponse(geminiResponse);
    responseBodyForLog = responseWithUrls;

    res.json(responseWithUrls);
    writeLog({ success: true, status: res.statusCode || 200 });
  } catch (error) {
    const status = 500;
    const message = error?.message || 'Gemini generateContent 调用失败';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
  }
};

const handleGeminiStreamGenerateContent = async (req, res) => {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const model = req.params.model || req.body?.model || 'unknown';

  // 兼容模型别名后缀 -1k/-2k/-4k：用于指定分辨率，发送给上游时去掉后缀
  let upstreamModel = model;
  let imageSizeFromModel = null;
  if (typeof model === 'string') {
    const match = model.match(/^(.*-image)(?:-(1k|2k|4k))$/i);
    if (match) {
      upstreamModel = match[1];
      imageSizeFromModel = match[2].toUpperCase(); // 1K/2K/4K
    }
  }

  let token = null;
  const streamEventsForLog = [];
  let responseBodyForLog = null;

  const writeLog = ({ success, status, message }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model,
      projectId: token?.projectId || null,
      success,
      status,
      message,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        }
      }
    });
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        },
        error: success ? undefined : message
      });
    }
  };

  try {
    const body = req.body || {};
    if (!Array.isArray(body.contents) || body.contents.length === 0) {
      const status = 400;
      const message = 'contents is required for Gemini streamGenerateContent';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    // 若通过模型后缀指定分辨率且请求未显式携带，则补全到 generationConfig.imageConfig.imageSize
    if (imageSizeFromModel) {
      const genCfg = body.generationConfig || {};
      const imgCfg = genCfg.imageConfig || {};
      const hasImageSize = imgCfg.imageSize || imgCfg.image_size;
      if (!hasImageSize) {
        imgCfg.imageSize = imageSizeFromModel;
        genCfg.imageConfig = imgCfg;
        body.generationConfig = genCfg;
      }
    }

    token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    const requestBody = generateRequestBodyFromGemini(body, upstreamModel, token);

    setStreamHeaders(res);
    res.flushHeaders();

    const sendSse = payload => {
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    const { usage } = await generateAssistantResponse(requestBody, token, data => {
      streamEventsForLog.push(data);
      if (data.type === 'thinking') {
        sendSse({ candidates: [{ content: { parts: [{ text: data.content, thought: true }] } }] });
      } else if (data.type === 'text') {
        sendSse({ candidates: [{ content: { parts: [{ text: data.content }] } }] });
      } else if (data.type === 'image') {
        sendSse({
          candidates: [
            {
              content: {
                parts: [
                  {
                    inlineData: {
                      mimeType: data.mimeType || 'image/png',
                      url: data.url,
                      data: data.data
                    }
                  }
                ]
              }
            }
          ]
        });
      } else if (data.type === 'tool_calls') {
        // Gemini 流式暂不下发工具调用，忽略
      }
    });

    sendSse({ done: true, usage: usage || null });
    res.end();

    responseBodyForLog = { stream: true, events: streamEventsForLog, usage };
    writeLog({ success: true, status: 200 });
  } catch (error) {
    const status = 500;
    const message = error?.message || 'Gemini streamGenerateContent 调用失败';
    if (!res.headersSent) {
      res.status(status).json({ error: message });
    } else {
      res.write(`data: ${JSON.stringify({ error: message })}\n\n`);
      res.end();
    }
    writeLog({ success: false, status, message });
  }
};

app.post('/v1beta/models/:model\\:generateContent', handleGeminiGenerateContent);
app.post('/v1beta/models/:model\\:streamGenerateContent', handleGeminiStreamGenerateContent);
// 兼容 README 中的 /gemini/v1beta 前缀
app.post('/gemini/v1beta/models/:model\\:generateContent', handleGeminiGenerateContent);
app.post('/gemini/v1beta/models/:model\\:streamGenerateContent', handleGeminiStreamGenerateContent);

// OpenAI 图像生成兼容接口：/v1/images/generations
app.post('/v1/images/generations', async (req, res) => {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const { prompt, model, size, user, response_format } = req.body || {};

  let token = null;
  let responseBodyForLog = null;
  const writeLog = ({ success, status, message }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model: model || 'unknown',
      projectId: token?.projectId || null,
      success,
      status,
      message,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        }
      }
    });
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        },
        error: success ? undefined : message
      });
    }
  };

  try {
    if (!prompt || !model) {
      const status = 400;
      const message = 'prompt 和 model 均为必填';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    // 将 OpenAI image size 映射到 image_size（1K/2K/4K）
    const sizeMap = {
      '256x256': '1K',
      '512x512': '1K',
      '1024x1024': '1K',
      '1536x1536': '2K',
      '2048x2048': '2K',
      '4096x4096': '4K'
    };
    const imageSize = sizeMap[String(size).toLowerCase()] || null;
    const params = {};
    if (imageSize) params.image_size = imageSize;

    token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    const messages = [{ role: 'user', content: prompt }];
    const requestBody = generateRequestBody(messages, model, params, undefined, token);
    // 图像模型固定 image_gen
    requestBody.requestType = 'image_gen';

    const { content } = await generateAssistantResponseNoStream(requestBody, token);
    // 提取 markdown 里的图片 URL 或直接解析 inlineData 生成的 URL
    const imageUrls = [];
    const urlRegex = /!\\[image\\]\\(([^)]+)\\)/g;
    let match;
    while ((match = urlRegex.exec(content || '')) !== null) {
      if (match[1]) imageUrls.push(match[1]);
    }

    if (imageUrls.length === 0) {
      const status = 502;
      const message = '上游未返回图片';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    const created = Math.floor(Date.now() / 1000);
    const data = imageUrls.map(url => {
      if (response_format === 'b64_json') {
        // 提示：当前未存储原始 base64，这里返回空字符串占位，避免 400
        return { b64_json: '' };
      }
      return { url };
    });

    const payload = { created, data };
    responseBodyForLog = payload;
    res.json(payload);
    writeLog({ success: true, status: res.statusCode || 200 });
  } catch (error) {
    const status = error?.statusCode || 500;
    const message = error?.message || '图片生成失败';
    if (!res.headersSent) {
      res.status(status).json({ error: message });
    }
    writeLog({ success: false, status, message });
  }
});

app.post('/v1/chat/completions', createChatCompletionHandler(
  // 传入 excludeIds 以支持重试时规避已失败的 token
  (req, excludeIds) => tokenManager.getToken(excludeIds)
));

app.post('/v1/messages/count_tokens', (req, res) => {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  let responseBodyForLog = null;

  const writeLog = ({ success, status, message }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model: req.body?.model || 'unknown',
      projectId: null,
      success,
      status,
      message,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        }
      }
    });
    // 同时输出到控制台详细日志
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        },
        error: success ? undefined : message
      });
    }
  };

  try {
    const result = countClaudeTokens(req.body || {});
    responseBodyForLog = result;
    res.json(result);
    writeLog({ success: true, status: res.statusCode || 200 });
  } catch (error) {
    const status = 400;
    const message = error?.message || '计算失败';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
  }
});

app.post('/v1/messages', async (req, res) => {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  let responseBodyForLog = null;
  let token = null;
  let requestBody = null;
  const claudeBody = req.body || {};
  const clientModelForLog = claudeBody.model;

  const writeLog = ({ success, status, message }) => {
    appendLog({
      timestamp: new Date().toISOString(),
      model: clientModelForLog || 'unknown',
      projectId: token?.projectId || null,
      success,
      status,
      message,
      durationMs: Date.now() - startedAt,
      path: req.originalUrl,
      method: req.method,
      detail: {
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        }
      }
    });
    // 同时输出到控制台详细日志
    if (logger.detail) {
      logger.detail({
        method: req.method,
        path: req.originalUrl,
        status,
        durationMs: Date.now() - startedAt,
        request: requestSnapshot,
        response: {
          status,
          headers: res.getHeaders ? res.getHeaders() : undefined,
          body: responseBodyForLog
        },
        error: success ? undefined : message
      });
    }
  };

  try {
    // 计算输入 token
    const tokenStats = (() => {
      try {
        return countClaudeTokens(claudeBody);
      } catch {
        return { input_tokens: 0 };
      }
    })();

    token = await tokenManager.getToken();
    if (!token) {
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(503).json({ error: message });
      writeLog({ success: false, status: 503, message });
      return;
    }

    // 直接使用 Anthropic → Gemini 转换，不再经过 OpenAI 中间层
    requestBody = generateRequestBodyFromAnthropic(claudeBody, token);
    const requestId = requestBody.requestId;
    const isStream = claudeBody.stream !== false;

    if (isStream) {
      setStreamHeaders(res);
      const emitter = new ClaudeSseEmitter(res, requestId, {
        model: claudeBody.model,
        inputTokens: tokenStats?.input_tokens || 0
      });
      emitter.start();

      let hasToolCalls = false;
      const { usage, finishReason } = await generateAssistantResponse(requestBody, token, async data => {
        if (data.type === 'thinking') {
          emitter.sendThinking(data.content);
        } else if (data.type === 'text') {
          emitter.sendText(data.content);
        } else if (data.type === 'image') {
          emitter.sendText(`![image](${data.url})`);
        } else if (data.type === 'tool_calls') {
          hasToolCalls = true;
          await emitter.sendToolCalls(data.tool_calls);
        }
      });

      // 使用上游返回的 finishReason (OpenAI 格式) 映射为 Claude stop_reason
      const stopReason = finishReason
        ? mapOpenAIFinishReasonToClaude(finishReason)
        : (hasToolCalls ? 'tool_use' : 'end_turn');

      responseBodyForLog = { stream: true, usage };
      emitter.finish(usage, stopReason);
      writeLog({ success: true, status: res.statusCode || 200 });
    } else {
      const result = await generateAssistantResponseNoStream(requestBody, token);
      const contentBlocks = buildClaudeContentBlocks(result.content, result.toolCalls);
      const outputTokens =
        result.usage?.completion_tokens ??
        result.usage?.output_tokens ??
        (result.content ? estimateTokensFromText(result.content) : 0);

      // 使用上游返回的 finishReason (OpenAI 格式) 映射为 Claude stop_reason
      const stopReason = result.finishReason
        ? mapOpenAIFinishReasonToClaude(result.finishReason)
        : (result.toolCalls?.length ? 'tool_use' : 'end_turn');

      const payload = {
        id: `msg_${requestId}`,
        type: 'message',
        role: 'assistant',
        model: claudeBody.model,
        content: contentBlocks,
        stop_reason: stopReason,
        stop_sequence: null,
        usage: {
          input_tokens: tokenStats?.input_tokens || 0,
          output_tokens: outputTokens || 0
        }
      };

      responseBodyForLog = payload;
      res.json(payload);
      writeLog({ success: true, status: res.statusCode || 200 });
    }
  } catch (error) {
    logger.error('/v1/messages 请求失败:', error?.message || error);
    const status = error?.statusCode || 500;
    if (!res.headersSent) {
      res.status(status).json({ error: error?.message || '服务器失败' });
    }
    writeLog({ success: false, status, message: error?.message });
  }
});

// ===== Server bootstrap =====

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
