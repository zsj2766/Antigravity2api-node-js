/**
 * 配置定义模块 (Settings Definitions)
 *
 * 职责：
 * - 定义所有可配置项的元数据
 * - 提供配置值的格式化和脱敏功能
 * - 构建管理面板的配置摘要
 *
 * 设计说明：
 * - 配置定义与业务逻辑分离，便于维护
 * - 支持多种配置来源：环境变量、数据文件、默认值
 * - 敏感配置自动脱敏显示
 *
 * @module config/settings
 */

import config from './config.js';
import {
  getEffectiveConfig as getEffectiveDataConfig,
  isDockerOnlyKey
} from './dataConfig.js';

/**
 * 配置项定义
 *
 * 每个配置项包含：
 * - key: 环境变量名
 * - label: 显示名称
 * - category: 分类（用于分组显示）
 * - defaultValue: 默认值
 * - description: 描述说明（可选）
 * - sensitive: 是否敏感（需脱敏）（可选）
 * - valueResolver: 从 config 对象获取值的函数（可选）
 *
 * @constant {Array<Object>}
 */
const SETTINGS_DEFINITIONS = [
  // ========== 限额与重试 ==========
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
  },

  // ========== 调用日志 ==========
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

  // ========== 面板与安全 ==========
  {
    key: 'PANEL_USER',
    label: '面板登录用户名',
    category: '面板与安全',
    defaultValue: 'admin',
    valueResolver: cfg => cfg.panelUser || 'admin'
  },
  {
    key: 'PANEL_PASSWORD',
    label: '面板登录密码',
    category: '面板与安全',
    defaultValue: null,
    sensitive: true,
    valueResolver: cfg => (cfg.panelPassword ? '已配置' : null),
    description: '用于保护���理界面，未配置将拒绝启动'
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

  // ========== 服务与网络 ==========
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
    valueResolver: cfg => cfg.server.host
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

  // ========== 生成参数 ==========
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
  }
];

/**
 * 配置项快速查找 Map
 *
 * 通过环境变量名快速获取配置定义。
 *
 * @type {Map<string, Object>}
 */
const SETTINGS_MAP = new Map(SETTINGS_DEFINITIONS.map(def => [def.key, def]));

/**
 * 标准化配置值
 *
 * 将各种类型的值转换为可显示的字符串格式。
 *
 * @param {*} value - 原始值
 * @returns {string|null} 标准化后的值
 *
 * @example
 * normalizeValue(['a', 'b']); // 'a, b'
 * normalizeValue({ foo: 1 }); // '{"foo":1}'
 */
function normalizeValue(value) {
  if (value === undefined || value === null) return null;
  if (Array.isArray(value)) return value.join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return value;
}

/**
 * 脱敏处理敏感值
 *
 * 将敏感信息（如密码、API Key）部分隐藏，
 * 仅显示首尾字符，中间用星号替代。
 *
 * @param {*} value - 原始值
 * @returns {string|null} 脱敏后的值
 *
 * @example
 * maskSecret('my-secret-key'); // 'my******ey'
 * maskSecret('abc');           // '****'
 */
function maskSecret(value) {
  if (value === undefined || value === null) return null;
  const str = String(value);
  if (!str) return null;
  if (str.length <= 4) return '****';
  return `${str.slice(0, 2)}${'*'.repeat(Math.max(4, str.length - 4))}${str.slice(-2)}`;
}

/**
 * 构建配置摘要
 *
 * 遍历所有配置定义，解析当前值和来源，
 * 按分类分组返回用于管理面板显示的数据结构。
 *
 * 配置优先级：Docker 环境变量 > data 文件 > 默认值
 *
 * @param {Object} [configSnapshot=config] - 配置对象快照
 * @returns {Array<Object>} 分组后的配置列表
 * @property {string} name - 分组名称
 * @property {Array} items - 该分组下的配置项
 *
 * @example
 * const groups = buildSettingsSummary();
 * // [{ name: '限额与重试', items: [...] }, ...]
 */
function buildSettingsSummary(configSnapshot = config) {
  const dataConfig = getEffectiveDataConfig();
  const groups = new Map();

  SETTINGS_DEFINITIONS.forEach(def => {
    const envValue = process.env[def.key];
    const dataValue = dataConfig[def.key];
    const envNormalized = normalizeValue(envValue);
    const dataNormalized = normalizeValue(dataValue);
    const defaultNormalized = normalizeValue(def.defaultValue ?? null);

    // 判断配置来源
    let source = 'default';
    let resolved = defaultNormalized;

    // Docker 专用配置只能从环境变量读取
    if (isDockerOnlyKey(def.key)) {
      if (envValue !== undefined && envValue !== null && envValue !== '') {
        source = 'docker';
        resolved = normalizeValue(envValue);
      }
    } else {
      // 其他配置：data 文件 > 默认值
      if (dataValue !== undefined && dataValue !== null && dataValue !== '') {
        source = 'file';
        resolved = dataNormalized;
      } else if (envValue !== undefined && envValue !== null && envValue !== '') {
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
      dockerOnly: isDockerOnlyKey(def.key)
    };

    const groupName = def.category || '未分组';
    if (!groups.has(groupName)) {
      groups.set(groupName, { name: groupName, items: [] });
    }
    groups.get(groupName).items.push(item);
  });

  return Array.from(groups.values());
}

/**
 * 构建完整的配置响应数据
 *
 * 用于 /admin/settings API 响应。
 *
 * @param {Object} [configSnapshot=config] - 配置对象快照
 * @returns {Object} 配置响应数据
 * @property {string} updatedAt - 更新时间（ISO 格式）
 * @property {Array} groups - 分组后的配置列表
 */
function buildSettingsPayload(configSnapshot = config) {
  return {
    updatedAt: new Date().toISOString(),
    groups: buildSettingsSummary(configSnapshot)
  };
}

/**
 * 获取配置项定义
 *
 * @param {string} key - 环境变量名
 * @returns {Object|undefined} 配置项定义
 */
function getSettingDefinition(key) {
  return SETTINGS_MAP.get(key);
}

/**
 * 获取所有配置项键名
 *
 * @returns {string[]} 配置项键名数组
 */
function getAllSettingKeys() {
  return SETTINGS_DEFINITIONS.map(def => def.key);
}

export {
  SETTINGS_DEFINITIONS,
  SETTINGS_MAP,
  normalizeValue,
  maskSecret,
  buildSettingsSummary,
  buildSettingsPayload,
  getSettingDefinition,
  getAllSettingKeys
};
