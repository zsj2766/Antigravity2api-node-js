/**
 * 账号服务模块 (Account Service)
 *
 * 职责：
 * - 管理 accounts.json 文件的读写
 * - 提供账号的 CRUD 操作
 * - 处理 TOML 格式账号导入
 * - 账号数据标准化和合并
 *
 * 设计说明：
 * - 与控制器解耦，可被多个控制器复用
 * - 封装文件系统操作，提供统一的错误处理
 * - 支持账号状态查询和使用量统计
 *
 * @module services/accountService
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import logger from '../utils/logger.js';
import { getUsageSummary } from '../utils/log_store.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

/**
 * 账号文件路径
 * @constant {string}
 */
const ACCOUNTS_FILE = path.join(__dirname, '..', '..', 'data', 'accounts.json');

/**
 * 安全读取账号列表
 *
 * 从 accounts.json 读取所有账号，并附加使用量统计。
 * 敏感信息（如 refresh_token）不会返回给前端。
 *
 * @returns {Array<Object>} 账号摘要列表
 * @property {number} index - 账号索引
 * @property {string|null} projectId - 项目 ID
 * @property {string|null} email - 用户邮箱
 * @property {boolean} enable - 是否启用
 * @property {boolean} hasRefreshToken - 是否有刷新令牌
 * @property {Object} usage - 使用量统计
 *
 * @example
 * const accounts = readAccountsSafe();
 * // [{ index: 0, projectId: 'xxx', email: 'user@example.com', ... }]
 */
export function readAccountsSafe() {
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

/**
 * 读取原始账号数据
 *
 * 返回完整的账号数据，包含敏感信息。
 * 仅供内部服务使用，不应直接返回给前端。
 *
 * @returns {Array<Object>} 原始账号数组
 */
export function readAccountsRaw() {
  try {
    if (!fs.existsSync(ACCOUNTS_FILE)) return [];
    const raw = fs.readFileSync(ACCOUNTS_FILE, 'utf-8');
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data : [];
  } catch (e) {
    logger.error(`读取 accounts.json 失败: ${e.message}`);
    return [];
  }
}

/**
 * 保存账号数据
 *
 * 将账号数组写入 accounts.json 文件。
 *
 * @param {Array<Object>} accounts - 账号数组
 * @returns {boolean} 是否保存成功
 */
export function saveAccounts(accounts) {
  try {
    const dir = path.dirname(ACCOUNTS_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), 'utf-8');
    return true;
  } catch (e) {
    logger.error(`保存 accounts.json 失败: ${e.message}`);
    return false;
  }
}

/**
 * 解析时间戳
 *
 * 从多种格式中解析时间戳。
 *
 * @param {Object} raw - 原始数据对象
 * @returns {number} Unix 时间戳（毫秒）
 */
export function parseTimestamp(raw) {
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

/**
 * 标准化 TOML 格式账号
 *
 * 将 TOML 导入的账号数据转换为统一格式。
 * 支持多种字段命名风格（camelCase 和 snake_case）。
 *
 * @param {Object} raw - 原始账号数据
 * @param {Object} options - 选项
 * @param {boolean} [options.filterDisabled=false] - 是否过滤禁用账号
 * @returns {Object|null} 标准化后的账号，无效时返回 null
 *
 * @example
 * const account = normalizeTomlAccount({
 *   access_token: 'xxx',
 *   refresh_token: 'yyy',
 *   projectId: 'my-project'
 * });
 */
export function normalizeTomlAccount(raw, { filterDisabled = false } = {}) {
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

  // 复制其他可选字段
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

/**
 * 合并账号列表
 *
 * 将新导入的账号与现有账号合并，基于 refresh_token 去重。
 *
 * @param {Array<Object>} existing - 现有账号列表
 * @param {Array<Object>} incoming - 新导入账号列表
 * @param {boolean} [replaceExisting=false] - 是否完全替换（true 时忽略现有账号）
 * @returns {Array<Object>} 合并后的账号列表
 *
 * @example
 * const merged = mergeAccounts(existingAccounts, newAccounts);
 */
export function mergeAccounts(existing, incoming, replaceExisting = false) {
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

/**
 * 根据索引获取账号
 *
 * @param {number} index - 账号索引
 * @returns {Object|null} 账号对象或 null
 */
export function getAccountByIndex(index) {
  const accounts = readAccountsRaw();
  if (index < 0 || index >= accounts.length) return null;
  return accounts[index];
}

/**
 * 更新账号状态
 *
 * @param {number} index - 账号索引
 * @param {boolean} enable - 是否启用
 * @returns {boolean} 是否更新成功
 */
export function updateAccountStatus(index, enable) {
  const accounts = readAccountsRaw();
  if (index < 0 || index >= accounts.length) return false;

  accounts[index].enable = enable;
  return saveAccounts(accounts);
}

/**
 * 删除账号
 *
 * @param {number} index - 账号索引
 * @returns {boolean} 是否删除成功
 */
export function deleteAccount(index) {
  const accounts = readAccountsRaw();
  if (index < 0 || index >= accounts.length) return false;

  accounts.splice(index, 1);
  return saveAccounts(accounts);
}

/**
 * 添加新账号
 *
 * @param {Object} account - 账号数据
 * @returns {boolean} 是否添加成功
 */
export function addAccount(account) {
  const accounts = readAccountsRaw();
  accounts.push(account);
  return saveAccounts(accounts);
}

/**
 * 更新指定索引的账号
 *
 * @param {number} index - 账号索引
 * @param {Object} updates - 要更新的字段
 * @returns {boolean} 是否更新成功
 */
export function updateAccount(index, updates) {
  const accounts = readAccountsRaw();
  if (index < 0 || index >= accounts.length) return false;

  accounts[index] = { ...accounts[index], ...updates };
  return saveAccounts(accounts);
}

/**
 * 替换指定索引的账号
 *
 * @param {number} index - 账号索引
 * @param {Object} account - 新账号数据
 * @returns {boolean} 是否替换成功
 */
export function replaceAccount(index, account) {
  const accounts = readAccountsRaw();
  if (index < 0 || index >= accounts.length) return false;

  accounts[index] = account;
  return saveAccounts(accounts);
}

export {
  ACCOUNTS_FILE
};

export default {
  readAccountsSafe,
  readAccountsRaw,
  saveAccounts,
  parseTimestamp,
  normalizeTomlAccount,
  mergeAccounts,
  getAccountByIndex,
  updateAccountStatus,
  deleteAccount,
  addAccount,
  updateAccount,
  replaceAccount
};
