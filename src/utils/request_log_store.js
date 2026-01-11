/**
 * 请求日志文件存储模块
 *
 * 每个请求存储为独立的 JSON 文件，包含完整的：
 * - 客户端请求数据
 * - 转换链请求/响应数据
 * - 上游 API 请求/响应数据
 * - 最终返回数据
 * - 错误信息
 *
 * @module utils/request_log_store
 */

import fs from 'fs';
import path from 'path';
import { randomUUID } from 'crypto';
import { fileURLToPath } from 'url';
import config from '../config/config.js';
// 导入旧日志系统用于保持 token_manager 统计兼容
import { appendLog as appendLegacyLog } from './log_store.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 日志目录
const LOG_DIR = config.logging.requestLogDetailDir
  ? path.resolve(config.logging.requestLogDetailDir)
  : path.join(__dirname, '..', '..', 'data', 'request_logs');

// 索引文件
const INDEX_FILE = path.join(path.dirname(LOG_DIR), 'request_logs_index.json');

// 配置
const RETENTION_DAYS = Math.max(1, config.logging.requestLogRetentionDays || 7);
const MAX_LOGS = config.logging.requestLogMaxItems || 10000;
const MAX_DATA_SIZE = 100 * 1024; // 100KB 单个字段最大长度

// 日志广播回调列表
const logListeners = [];

/**
 * 确保目录存在
 */
function ensureDir() {
  if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  }
}

/**
 * 获取日志级别
 */
function getLogLevel() {
  const raw = (config.logging.requestLogLevel || '').toLowerCase();
  if (raw === 'off' || raw === 'error' || raw === 'all') return raw;
  return 'all';
}

/**
 * 是否应该记录日志
 */
function shouldLogEntry(entry) {
  const level = getLogLevel();
  if (level === 'off') return false;
  if (level === 'error') {
    const status = Number(entry?.status);
    const success = entry?.success;
    const isErrorStatus = Number.isFinite(status) && status >= 400;
    const isFailed = success === false;
    return isErrorStatus || isFailed;
  }
  return true;
}

/**
 * 截断大型数据
 */
function truncateData(data, maxSize = MAX_DATA_SIZE) {
  if (data === null || data === undefined) return data;

  let jsonStr;
  try {
    jsonStr = typeof data === 'string' ? data : JSON.stringify(data);
  } catch {
    return '[无法序列化]';
  }

  if (jsonStr.length <= maxSize) {
    return data;
  }

  // 截断并标记
  const truncatedStr = jsonStr.slice(0, maxSize);
  return {
    _truncated: true,
    _originalLength: jsonStr.length,
    data: truncatedStr + '...[TRUNCATED]'
  };
}

/**
 * 脱敏处理
 */
function sanitize(data) {
  if (data === null || data === undefined) return data;

  if (typeof data === 'string') {
    // 检查是否为 base64
    if (data.length > 200 && /^[A-Za-z0-9+/=]+$/.test(data.slice(0, 100))) {
      return `[BASE64:${data.length} chars]`;
    }
    return data;
  }

  if (Array.isArray(data)) {
    return data.map(item => sanitize(item));
  }

  if (typeof data === 'object') {
    const result = {};
    const sensitiveKeys = ['password', 'token', 'secret', 'key', 'authorization', 'cookie', 'access_token', 'refresh_token'];

    for (const [key, value] of Object.entries(data)) {
      // 隐藏敏感字段
      if (sensitiveKeys.some(k => key.toLowerCase().includes(k))) {
        result[key] = '[REDACTED]';
        continue;
      }

      // 处理 base64 数据字段
      if ((key === 'data' || key === 'inline_data' || key === 'inlineData') &&
          typeof value === 'string' && value.length > 200) {
        result[key] = `[BASE64:${value.length} chars]`;
        continue;
      }

      result[key] = sanitize(value);
    }

    return result;
  }

  return data;
}

/**
 * 生成日志文件名
 */
function generateLogFileName(requestId, timestamp) {
  const date = new Date(timestamp);
  const dateStr = date.toISOString().slice(0, 10).replace(/-/g, '');
  const timeStr = date.toISOString().slice(11, 19).replace(/:/g, '');
  return `${dateStr}_${timeStr}_${requestId}.json`;
}

/**
 * 读取索引文件
 */
function readIndex() {
  try {
    if (!fs.existsSync(INDEX_FILE)) return [];
    const raw = fs.readFileSync(INDEX_FILE, 'utf-8');
    return JSON.parse(raw) || [];
  } catch {
    return [];
  }
}

/**
 * 写入索引文件
 */
function writeIndex(index) {
  try {
    ensureDir();
    fs.writeFileSync(INDEX_FILE, JSON.stringify(index, null, 2), 'utf-8');
  } catch (err) {
    console.error('[RequestLogStore] 写入索引失败:', err.message);
  }
}

/**
 * 清理过期日志
 */
function cleanupOldLogs() {
  const cutoff = Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000;
  const index = readIndex();

  let deletedCount = 0;
  const retained = index.filter(entry => {
    const timestamp = Date.parse(entry.timestamp);
    if (Number.isNaN(timestamp) || timestamp < cutoff) {
      // 删除文件
      try {
        const filePath = path.join(LOG_DIR, entry.fileName);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          deletedCount++;
        }
      } catch { /* ignore */ }
      return false;
    }
    return true;
  });

  // 超过最大数量时删除最旧的
  if (retained.length > MAX_LOGS) {
    const toDelete = retained.slice(0, retained.length - MAX_LOGS);
    toDelete.forEach(entry => {
      try {
        const filePath = path.join(LOG_DIR, entry.fileName);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
          deletedCount++;
        }
      } catch { /* ignore */ }
    });
    retained.splice(0, retained.length - MAX_LOGS);
  }

  if (deletedCount > 0) {
    writeIndex(retained);
  }

  return deletedCount;
}

/**
 * 请求日志构建器
 * 用于在请求生命周期中逐步收集数据
 */
export class RequestLogBuilder {
  constructor(requestId = null) {
    this.requestId = requestId || randomUUID();
    this.timestamp = new Date().toISOString();
    this.startTime = Date.now();

    // 基本信息
    this.meta = {
      id: this.requestId,
      timestamp: this.timestamp,
      success: null,
      status: null,
      durationMs: null,
      message: null,
      path: null,
      method: null,
      model: null,
      projectId: null,
      correlationId: null
    };

    // 客户端请求
    this.clientRequest = null;

    // 转换链
    this.pipeline = {
      stages: [],
      errors: []
    };

    // 上游请求/响应
    this.upstream = {
      request: null,
      response: null,
      error: null
    };

    // 最终响应
    this.clientResponse = null;
  }

  /**
   * 设置基本信息
   */
  setMeta(data) {
    Object.assign(this.meta, data);
    return this;
  }

  /**
   * 记录客户端请求
   */
  setClientRequest(req) {
    this.clientRequest = {
      method: req.method,
      path: req.originalUrl || req.url,
      headers: sanitize(req.headers),
      body: sanitize(truncateData(req.body)),
      query: req.query,
      ip: req.headers['x-forwarded-for'] || req.headers['x-real-ip'] || req.ip
    };
    this.meta.path = this.clientRequest.path;
    this.meta.method = this.clientRequest.method;
    return this;
  }

  /**
   * 记录转换阶段
   */
  addPipelineStage(name, input, output, metadata = {}) {
    const now = Date.now();
    const lastStage = this.pipeline.stages[this.pipeline.stages.length - 1];
    const durationMs = lastStage ? now - lastStage._timestamp : now - this.startTime;

    this.pipeline.stages.push({
      name,
      timestamp: new Date(now).toISOString(),
      durationMs,
      input: sanitize(truncateData(input)),
      output: sanitize(truncateData(output)),
      _timestamp: now,
      ...metadata
    });
    return this;
  }

  /**
   * 记录转换错误
   */
  addPipelineError(stage, error, input = null) {
    this.pipeline.errors.push({
      stage,
      timestamp: new Date().toISOString(),
      error: error?.message || String(error),
      code: error?.code,
      stack: error?.stack?.split('\n').slice(0, 5).join('\n'),
      input: input ? sanitize(truncateData(input)) : null
    });
    return this;
  }

  /**
   * 记录上游请求
   */
  setUpstreamRequest(url, method, headers, body) {
    this.upstream.request = {
      url,
      method,
      headers: sanitize(headers),
      body: sanitize(truncateData(body)),
      timestamp: new Date().toISOString()
    };
    return this;
  }

  /**
   * 记录上游响应
   */
  setUpstreamResponse(status, headers, body, durationMs = null) {
    this.upstream.response = {
      status,
      headers: sanitize(headers),
      body: sanitize(truncateData(body)),
      durationMs,
      timestamp: new Date().toISOString()
    };
    return this;
  }

  /**
   * 记录上游错误
   */
  setUpstreamError(error) {
    this.upstream.error = {
      message: error?.message || String(error),
      code: error?.code,
      status: error?.status || error?.statusCode,
      stack: error?.stack?.split('\n').slice(0, 5).join('\n'),
      rawResponse: error?.rawResponse ? sanitize(truncateData(error.rawResponse)) : null,
      timestamp: new Date().toISOString()
    };
    return this;
  }

  /**
   * 记录客户端响应
   */
  setClientResponse(status, headers, body, summary = null) {
    this.clientResponse = {
      status,
      headers: sanitize(headers),
      body: sanitize(truncateData(body)),
      summary: summary ? sanitize(truncateData(summary)) : null,
      timestamp: new Date().toISOString()
    };
    return this;
  }

  /**
   * 完成日志记录并保存
   */
  finish(options = {}) {
    const { success, status, message } = options;

    this.meta.success = success ?? this.meta.success;
    this.meta.status = status ?? this.meta.status ?? this.clientResponse?.status;
    this.meta.message = message ?? this.meta.message;
    this.meta.durationMs = Date.now() - this.startTime;

    // 清理内部时间戳
    this.pipeline.stages.forEach(stage => {
      delete stage._timestamp;
    });

    // 保存日志
    return saveRequestLog(this);
  }

  /**
   * 转换为日志对象
   */
  toLogObject() {
    return {
      meta: this.meta,
      clientRequest: this.clientRequest,
      pipeline: this.pipeline,
      upstream: this.upstream,
      clientResponse: this.clientResponse
    };
  }
}

/**
 * 保存请求日志到文件
 */
export function saveRequestLog(builder) {
  const shouldLog = shouldLogEntry(builder.meta);
  const fileName = generateLogFileName(builder.requestId, builder.timestamp);

  // 1. 同步到旧日志系统 (保持 token_manager 统计功能)
  syncToLegacyLog(builder);

  ensureDir();

  // 2. 如果应该记录详细日志，则写入文件
  if (shouldLog) {
    const logObject = builder.toLogObject();
    const filePath = path.join(LOG_DIR, fileName);
    try {
      fs.writeFileSync(filePath, JSON.stringify(logObject, null, 2), 'utf-8');
    } catch (err) {
      console.error('[RequestLogStore] 写入日志文件失败:', err.message);
    }
  }

  // 3. 更新索引（无论是否记录详情，都写入索引以保证统计准确性）
  const index = readIndex();
  const indexEntry = {
    id: builder.requestId,
    fileName: shouldLog ? fileName : null,
    timestamp: builder.timestamp,
    success: builder.meta.success,
    status: builder.meta.status,
    durationMs: builder.meta.durationMs,
    path: builder.meta.path,
    method: builder.meta.method,
    model: builder.meta.model,
    projectId: builder.meta.projectId,
    message: builder.meta.message,
    hasError: builder.pipeline.errors.length > 0 || !!builder.upstream.error,
    hasDetail: shouldLog
  };

  index.push(indexEntry);
  writeIndex(index);

  // 广播日志
  notifyLogListeners(indexEntry);

  // 定期清理（每 100 条日志检查一次）
  if (index.length % 100 === 0) {
    cleanupOldLogs();
  }

  return indexEntry;
}

/**
 * 同步日志到旧日志系统（用于 token_manager 统计）
 * 只同步必要的统计字段，不存储详细数据
 */
function syncToLegacyLog(builder) {
  try {
    appendLegacyLog({
      id: builder.requestId,
      timestamp: builder.timestamp,
      model: builder.meta.model,
      projectId: builder.meta.projectId,
      success: builder.meta.success,
      status: builder.meta.status,
      durationMs: builder.meta.durationMs,
      path: builder.meta.path,
      method: builder.meta.method,
      // 标记为仅用于统计，不存储详情
      trackUsage: true
    });
  } catch (err) {
    // 旧日志系统同步失败不影响主流程
    console.warn('[RequestLogStore] 同步到旧日志系统失败:', err.message);
  }
}

/**
 * 获取最近日志列表
 */
export function getRecentLogs(options = {}) {
  const {
    limit = 200,
    offset = 0,
    model,
    success,
    projectId,
    startTime,
    endTime
  } = options;

  let index = readIndex();

  // 筛选
  if (model) {
    index = index.filter(entry => entry.model === model);
  }
  if (success !== undefined) {
    index = index.filter(entry => entry.success === success);
  }
  if (projectId) {
    index = index.filter(entry => entry.projectId === projectId);
  }
  if (startTime) {
    index = index.filter(entry => entry.timestamp >= startTime);
  }
  if (endTime) {
    index = index.filter(entry => entry.timestamp <= endTime);
  }

  // 按时间倒序
  index.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

  // 分页
  return index.slice(offset, offset + limit);
}

/**
 * 获取日志总数
 */
export function getLogCount(options = {}) {
  const { model, success, projectId, startTime, endTime } = options;
  let index = readIndex();

  if (model) {
    index = index.filter(entry => entry.model === model);
  }
  if (success !== undefined) {
    index = index.filter(entry => entry.success === success);
  }
  if (projectId) {
    index = index.filter(entry => entry.projectId === projectId);
  }
  if (startTime) {
    index = index.filter(entry => entry.timestamp >= startTime);
  }
  if (endTime) {
    index = index.filter(entry => entry.timestamp <= endTime);
  }

  return index.length;
}

/**
 * 获取日志详情
 */
export function getLogDetail(id) {
  if (!id) return null;

  const index = readIndex();
  const entry = index.find(e => e.id === id);
  if (!entry) return null;

  // 如果没有详情文件（hasDetail=false 或 fileName=null），返回索引数据
  if (!entry.hasDetail || !entry.fileName) {
    return {
      meta: {
        id: entry.id,
        timestamp: entry.timestamp,
        success: entry.success,
        status: entry.status,
        durationMs: entry.durationMs,
        path: entry.path,
        method: entry.method,
        model: entry.model,
        projectId: entry.projectId,
        message: entry.message
      },
      clientRequest: null,
      pipeline: { stages: [], errors: [] },
      upstream: { request: null, response: null, error: null },
      clientResponse: null,
      _noDetail: true,
      _noDetailReason: '此日志未记录详情（日志级别设置为 error 或 off）'
    };
  }

  const filePath = path.join(LOG_DIR, entry.fileName);
  if (!fs.existsSync(filePath)) return null;

  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

/**
 * 清空所有日志
 */
export function clearLogs() {
  try {
    const index = readIndex();

    // 删除所有日志文件
    index.forEach(entry => {
      try {
        const filePath = path.join(LOG_DIR, entry.fileName);
        if (fs.existsSync(filePath)) {
          fs.unlinkSync(filePath);
        }
      } catch { /* ignore */ }
    });

    // 清空索引
    writeIndex([]);
    return true;
  } catch {
    return false;
  }
}

/**
 * 获取统计信息
 */
export function getStats() {
  const index = readIndex();

  let totalSize = 0;
  index.forEach(entry => {
    try {
      const filePath = path.join(LOG_DIR, entry.fileName);
      if (fs.existsSync(filePath)) {
        totalSize += fs.statSync(filePath).size;
      }
    } catch { /* ignore */ }
  });

  const sizeMB = (totalSize / 1024 / 1024).toFixed(2);

  return {
    totalLogs: index.length,
    totalSizeBytes: totalSize,
    totalSizeMB: sizeMB,
    // 兼容旧 API 格式
    dbSizeBytes: totalSize,
    dbSizeMB: sizeMB,
    dbPath: LOG_DIR,
    logDir: LOG_DIR,
    retentionDays: RETENTION_DAYS,
    maxLogs: MAX_LOGS,
    pipelineLogLevel: getLogLevel().toUpperCase()
  };
}

/**
 * 获取时间窗口内的使用统计
 */
export function getUsageCountsWithinWindow(windowMs = 60 * 60 * 1000) {
  const since = Date.now() - Math.abs(windowMs);
  const sinceStr = new Date(since).toISOString();

  const index = readIndex();
  const summary = {};

  index.forEach(entry => {
    if (entry.timestamp < sinceStr) return;

    const key = entry.projectId || '未知项目';
    if (!summary[key]) {
      summary[key] = { count: 0, success: 0, failed: 0, lastUsedAt: null };
    }

    summary[key].count += 1;
    summary[key].lastUsedAt = entry.timestamp;
    if (entry.success) {
      summary[key].success += 1;
    } else {
      summary[key].failed += 1;
    }
  });

  return Object.entries(summary)
    .map(([projectId, stats]) => ({ projectId, ...stats }))
    .sort((a, b) => b.count - a.count);
}

/**
 * 获取指定项目在指定时间之后的使用次数
 */
export function getUsageCountSince(projectId, sinceTimestampMs) {
  if (!projectId) return 0;

  const since = Number.isFinite(Number(sinceTimestampMs))
    ? Number(sinceTimestampMs)
    : Date.now() - 60 * 60 * 1000;
  const sinceStr = new Date(since).toISOString();

  const index = readIndex();
  return index.filter(entry =>
    entry.projectId === projectId &&
    entry.timestamp >= sinceStr &&
    entry.success === true
  ).length;
}

/**
 * 通知日志监听器
 */
function notifyLogListeners(logEntry) {
  for (const listener of logListeners) {
    try {
      listener(logEntry);
    } catch { /* ignore */ }
  }
}

/**
 * 注册日志监听器
 */
export function onLogAppended(callback) {
  if (typeof callback === 'function') {
    logListeners.push(callback);
    return () => {
      const index = logListeners.indexOf(callback);
      if (index > -1) {
        logListeners.splice(index, 1);
      }
    };
  }
  return () => {};
}

export default {
  RequestLogBuilder,
  saveRequestLog,
  getRecentLogs,
  getLogCount,
  getLogDetail,
  clearLogs,
  getStats,
  getUsageCountsWithinWindow,
  getUsageCountSince,
  cleanupOldLogs,
  onLogAppended
};
