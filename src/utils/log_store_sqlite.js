/**
 * SQLite 日志存储模块
 *
 * 提供基于 SQLite 的日志存储功能，支持：
 * - Pipeline 追踪（记录转换链路各阶段）
 * - 复杂查询（按模型、状态、时间范围筛选）
 * - 高性能并发读写（WAL 模式）
 *
 * @module utils/log_store_sqlite
 */

import Database from 'better-sqlite3';
import path from 'path';
import fs from 'fs';
import { randomUUID } from 'crypto';
import { fileURLToPath } from 'url';
import config from '../config/config.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// 数据库文件路径
const DB_PATH = config.logging.sqliteDbPath
  ? path.resolve(config.logging.sqliteDbPath)
  : path.join(__dirname, '..', '..', 'data', 'logs.db');

// 确保数据目录存在
const DB_DIR = path.dirname(DB_PATH);
if (!fs.existsSync(DB_DIR)) {
  fs.mkdirSync(DB_DIR, { recursive: true });
}

// 配置常量
const RETENTION_DAYS = Math.max(1, config.logging.requestLogRetentionDays || 7);
const MAX_DETAIL_SIZE = 500 * 1024; // 500KB，增加以容纳 base64 图片数据
const BATCH_SIZE = 10; // 批量写入阈值
const BATCH_INTERVAL_MS = 500; // 批量写入间隔

// 日志监听器列表（用于实时广播）
const logListeners = [];

// 日志级别
function getPipelineLogLevel() {
  const raw = (process.env.PIPELINE_LOG_LEVEL || config.logging.pipelineLogLevel || 'full').toLowerCase();
  if (['off', 'summary', 'full', 'error'].includes(raw)) return raw;
  return 'full';
}

// 初始化数据库
let db = null;

function getDb() {
  if (db) return db;

  db = new Database(DB_PATH);

  // 启用 WAL 模式（并发读写）
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('auto_vacuum = INCREMENTAL');

  // 创建表
  db.exec(`
    CREATE TABLE IF NOT EXISTS logs (
      id TEXT PRIMARY KEY,
      timestamp TEXT NOT NULL,
      model TEXT,
      project_id TEXT,
      success INTEGER,
      status INTEGER,
      duration_ms INTEGER,
      path TEXT,
      method TEXT,
      message TEXT,
      correlation_id TEXT,
      is_retry INTEGER DEFAULT 0,
      retry_count INTEGER DEFAULT 0,
      -- Pipeline 追踪
      pipeline_stages TEXT,
      pipeline_errors TEXT,
      pipeline_duration_ms INTEGER,
      -- 请求/响应详情
      detail TEXT,
      is_truncated INTEGER DEFAULT 0,
      -- 用量追踪
      usage_only INTEGER DEFAULT 0,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    -- 索引
    CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
    CREATE INDEX IF NOT EXISTS idx_logs_timestamp_status ON logs(timestamp DESC, status);
    CREATE INDEX IF NOT EXISTS idx_logs_model_timestamp ON logs(model, timestamp DESC);
    CREATE INDEX IF NOT EXISTS idx_logs_project ON logs(project_id);
    CREATE INDEX IF NOT EXISTS idx_logs_success ON logs(success);
    CREATE INDEX IF NOT EXISTS idx_logs_correlation ON logs(correlation_id);
  `);

  return db;
}

// 写入缓冲区
let writeBuffer = [];
let flushTimer = null;

/**
 * 截断大型数据以防止数据库膨胀
 */
function truncateData(data, maxSize = MAX_DETAIL_SIZE) {
  if (!data) return { data: null, truncated: false };

  let jsonStr;
  try {
    jsonStr = typeof data === 'string' ? data : JSON.stringify(data);
  } catch {
    return { data: null, truncated: false };
  }

  if (jsonStr.length <= maxSize) {
    return { data: jsonStr, truncated: false };
  }

  // 截断并添加标记
  const truncatedJson = jsonStr.slice(0, maxSize - 50) + '..."[TRUNCATED]"}';
  return { data: truncatedJson, truncated: true };
}

/**
 * 根据日志级别过滤 pipeline 数据
 */
function filterPipelineData(pipeline, hasError) {
  const level = getPipelineLogLevel();

  if (level === 'off') {
    return { stages: null, errors: null };
  }

  if (level === 'error' && !hasError) {
    return { stages: null, errors: null };
  }

  if (level === 'summary') {
    // 只保留阶段名称和耗时
    const stages = pipeline?.stages?.map(s => ({
      name: s.name,
      timestamp: s.timestamp,
      durationMs: s.durationMs,
      success: !s.error
    }));
    return {
      stages: stages ? JSON.stringify(stages) : null,
      errors: pipeline?.errors ? JSON.stringify(pipeline.errors) : null
    };
  }

  // full: 完整记录
  return {
    stages: pipeline?.stages ? JSON.stringify(pipeline.stages) : null,
    errors: pipeline?.errors ? JSON.stringify(pipeline.errors) : null
  };
}

/**
 * 刷新写入缓冲区到数据库
 */
function flushBuffer() {
  if (writeBuffer.length === 0) return;

  const database = getDb();
  const insert = database.prepare(`
    INSERT INTO logs (
      id, timestamp, model, project_id, success, status, duration_ms,
      path, method, message, correlation_id, is_retry, retry_count,
      pipeline_stages, pipeline_errors, pipeline_duration_ms,
      detail, is_truncated, usage_only
    ) VALUES (
      @id, @timestamp, @model, @project_id, @success, @status, @duration_ms,
      @path, @method, @message, @correlation_id, @is_retry, @retry_count,
      @pipeline_stages, @pipeline_errors, @pipeline_duration_ms,
      @detail, @is_truncated, @usage_only
    )
  `);

  const insertMany = database.transaction((entries) => {
    for (const entry of entries) {
      insert.run(entry);
    }
  });

  try {
    insertMany(writeBuffer);
  } catch (err) {
    console.error('[LogStore] 批量写入失败:', err.message);
  }

  writeBuffer = [];
}

/**
 * 调度批量写入
 */
function scheduleBatchWrite(entry) {
  writeBuffer.push(entry);

  // 达到阈值立即刷新
  if (writeBuffer.length >= BATCH_SIZE) {
    if (flushTimer) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    flushBuffer();
    return;
  }

  // 定时刷新
  if (!flushTimer) {
    flushTimer = setTimeout(() => {
      flushTimer = null;
      flushBuffer();
    }, BATCH_INTERVAL_MS);
  }
}

/**
 * 追加日志条目
 *
 * @param {Object} entry - 日志条目
 * @param {Object} [entry.pipeline] - Pipeline 追踪数据
 * @param {Array} [entry.pipeline.stages] - 各阶段记录
 * @param {Array} [entry.pipeline.errors] - 错误记录
 * @returns {Object|null} 写入的日志条目
 */
export function appendLog(entry) {
  if (!entry) return null;

  const id = entry.id || randomUUID();
  const timestamp = entry.timestamp || new Date().toISOString();
  const hasError = entry.success === false || (entry.pipeline?.errors?.length > 0);

  // 处理 pipeline 数据
  const { stages, errors } = filterPipelineData(entry.pipeline, hasError);

  // 处理详情数据
  const { data: detail, truncated } = truncateData(entry.detail);

  const dbEntry = {
    id,
    timestamp,
    model: entry.model || null,
    project_id: entry.projectId || null,
    success: entry.success === true ? 1 : (entry.success === false ? 0 : null),
    status: entry.status || null,
    duration_ms: entry.durationMs || null,
    path: entry.path || null,
    method: entry.method || null,
    message: entry.message || null,
    correlation_id: entry.correlationId || null,
    is_retry: entry.isRetry ? 1 : 0,
    retry_count: entry.retryCount || 0,
    pipeline_stages: stages,
    pipeline_errors: errors,
    pipeline_duration_ms: entry.pipeline?.totalDurationMs || null,
    detail,
    is_truncated: truncated ? 1 : 0,
    usage_only: entry.usageOnly ? 1 : 0
  };

  scheduleBatchWrite(dbEntry);

  // 通知监听器（用于实时广播）
  notifyLogListeners({
    id,
    timestamp,
    model: entry.model,
    projectId: entry.projectId,
    success: entry.success,
    status: entry.status,
    durationMs: entry.durationMs,
    path: entry.path,
    method: entry.method,
    message: entry.message,
    correlationId: entry.correlationId,
    hasDetail: !!entry.detail
  });

  return { ...entry, id, timestamp };
}

/**
 * 获取最近日志
 *
 * @param {Object} options - 查询选项
 * @param {number} [options.limit=200] - 返回数量限制
 * @param {number} [options.offset=0] - 偏移量
 * @param {string} [options.model] - 按模型筛选
 * @param {boolean} [options.success] - 按成功状态筛选
 * @param {string} [options.projectId] - 按项目筛选
 * @param {string} [options.startTime] - 开始时间
 * @param {string} [options.endTime] - 结束时间
 * @returns {Array} 日志列表
 */
export function getRecentLogs(options = {}) {
  // 确保缓冲区已刷新
  flushBuffer();

  const {
    limit = 200,
    offset = 0,
    model,
    success,
    projectId,
    startTime,
    endTime
  } = options;

  const database = getDb();
  const conditions = ['usage_only = 0'];
  const params = {};

  if (model) {
    conditions.push('model = @model');
    params.model = model;
  }

  if (success !== undefined) {
    conditions.push('success = @success');
    params.success = success ? 1 : 0;
  }

  if (projectId) {
    conditions.push('project_id = @project_id');
    params.project_id = projectId;
  }

  if (startTime) {
    conditions.push('timestamp >= @start_time');
    params.start_time = startTime;
  }

  if (endTime) {
    conditions.push('timestamp <= @end_time');
    params.end_time = endTime;
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';

  const query = `
    SELECT id, timestamp, model, project_id, success, status, duration_ms,
           path, method, message, correlation_id, is_retry, retry_count,
           pipeline_duration_ms, is_truncated,
           CASE WHEN detail IS NOT NULL THEN 1 ELSE 0 END as has_detail,
           CASE WHEN pipeline_stages IS NOT NULL THEN 1 ELSE 0 END as has_pipeline
    FROM logs
    ${whereClause}
    ORDER BY timestamp DESC
    LIMIT @limit OFFSET @offset
  `;

  params.limit = limit;
  params.offset = offset;

  const stmt = database.prepare(query);
  const rows = stmt.all(params);

  return rows.map(row => ({
    id: row.id,
    timestamp: row.timestamp,
    model: row.model,
    projectId: row.project_id,
    success: row.success === 1,
    status: row.status,
    durationMs: row.duration_ms,
    path: row.path,
    method: row.method,
    message: row.message,
    correlationId: row.correlation_id,
    isRetry: row.is_retry === 1,
    retryCount: row.retry_count,
    pipelineDurationMs: row.pipeline_duration_ms,
    isTruncated: row.is_truncated === 1,
    hasDetail: row.has_detail === 1,
    hasPipeline: row.has_pipeline === 1
  }));
}

/**
 * 获取日志详情
 *
 * @param {string} id - 日志 ID
 * @returns {Object|null} 日志详情
 */
export function getLogDetail(id) {
  if (!id) return null;

  // 确保缓冲区已刷新
  flushBuffer();

  const database = getDb();
  const stmt = database.prepare('SELECT * FROM logs WHERE id = ?');
  const row = stmt.get(id);

  if (!row) return null;

  let detail = null;
  let pipelineStages = null;
  let pipelineErrors = null;

  try {
    if (row.detail) detail = JSON.parse(row.detail);
  } catch { /* ignore */ }

  try {
    if (row.pipeline_stages) pipelineStages = JSON.parse(row.pipeline_stages);
  } catch { /* ignore */ }

  try {
    if (row.pipeline_errors) pipelineErrors = JSON.parse(row.pipeline_errors);
  } catch { /* ignore */ }

  return {
    id: row.id,
    timestamp: row.timestamp,
    model: row.model,
    projectId: row.project_id,
    success: row.success === 1,
    status: row.status,
    durationMs: row.duration_ms,
    path: row.path,
    method: row.method,
    message: row.message,
    correlationId: row.correlation_id,
    isRetry: row.is_retry === 1,
    retryCount: row.retry_count,
    isTruncated: row.is_truncated === 1,
    detail,
    pipeline: {
      stages: pipelineStages,
      errors: pipelineErrors,
      totalDurationMs: row.pipeline_duration_ms
    }
  };
}

/**
 * 获取指定时间窗口内的用量统计
 *
 * @param {number} [windowMs=3600000] - 时间窗口（毫秒）
 * @returns {Array} 用量统计
 */
export function getUsageCountsWithinWindow(windowMs = 60 * 60 * 1000) {
  flushBuffer();

  const database = getDb();
  const since = new Date(Date.now() - Math.abs(windowMs)).toISOString();

  const stmt = database.prepare(`
    SELECT project_id,
           COUNT(*) as count,
           SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
           SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed_count,
           MAX(timestamp) as last_used_at
    FROM logs
    WHERE timestamp >= ?
    GROUP BY project_id
    ORDER BY count DESC
  `);

  const rows = stmt.all(since);

  return rows.map(row => ({
    projectId: row.project_id || '未知项目',
    count: row.count,
    success: row.success_count,
    failed: row.failed_count,
    lastUsedAt: row.last_used_at
  }));
}

/**
 * 获取指定项目在指定时间之后的使用次数
 */
export function getUsageCountSince(projectId, sinceTimestampMs) {
  if (!projectId) return 0;

  flushBuffer();

  const database = getDb();
  const since = new Date(
    Number.isFinite(Number(sinceTimestampMs))
      ? Number(sinceTimestampMs)
      : Date.now() - 60 * 60 * 1000
  ).toISOString();

  const stmt = database.prepare(`
    SELECT COUNT(*) as count
    FROM logs
    WHERE project_id = ? AND timestamp >= ? AND success = 1
  `);

  const row = stmt.get(projectId, since);
  return row?.count || 0;
}

/**
 * 获取 Token 使用统计
 */
export function getRecentTokenStats() {
  flushBuffer();

  const database = getDb();
  const stmt = database.prepare(`
    SELECT project_id,
           MAX(timestamp) as last_used,
           MAX(CASE WHEN success = 0 AND status = 429 THEN timestamp ELSE NULL END) as last_failure,
           SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success_count,
           SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failure_count
    FROM logs
    GROUP BY project_id
  `);

  const rows = stmt.all();
  const stats = {};

  for (const row of rows) {
    if (!row.project_id) continue;
    stats[row.project_id] = {
      lastUsed: row.last_used ? Date.parse(row.last_used) : 0,
      lastFailure: row.last_failure ? Date.parse(row.last_failure) : 0,
      successCount: row.success_count,
      failureCount: row.failure_count
    };
  }

  return stats;
}

/**
 * 获取使用摘要
 */
export function getUsageSummary() {
  flushBuffer();

  const database = getDb();
  const stmt = database.prepare(`
    SELECT project_id,
           COUNT(*) as total,
           SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as success,
           SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed,
           MAX(timestamp) as last_used_at,
           GROUP_CONCAT(DISTINCT model) as models
    FROM logs
    GROUP BY project_id
  `);

  const rows = stmt.all();
  const summary = {};

  for (const row of rows) {
    const key = row.project_id || '未知项目';
    summary[key] = {
      total: row.total,
      success: row.success,
      failed: row.failed,
      lastUsedAt: row.last_used_at,
      models: row.models ? row.models.split(',').filter(Boolean) : []
    };
  }

  return summary;
}

/**
 * 清理过期日志
 */
export function cleanupOldLogs() {
  flushBuffer();

  const database = getDb();
  const cutoff = new Date(Date.now() - RETENTION_DAYS * 24 * 60 * 60 * 1000).toISOString();

  const stmt = database.prepare('DELETE FROM logs WHERE timestamp < ?');
  const result = stmt.run(cutoff);

  // 增量 VACUUM 回收空间
  database.pragma('incremental_vacuum');

  return result.changes;
}

/**
 * 清空所有日志
 */
export function clearLogs() {
  flushBuffer();

  const database = getDb();
  database.exec('DELETE FROM logs');
  database.pragma('vacuum');

  return true;
}

/**
 * 获取数据库统计信息
 */
export function getDbStats() {
  flushBuffer();

  const database = getDb();

  const countStmt = database.prepare('SELECT COUNT(*) as count FROM logs');
  const sizeStmt = database.prepare('SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()');

  const count = countStmt.get()?.count || 0;
  const size = sizeStmt.get()?.size || 0;

  return {
    totalLogs: count,
    dbSizeBytes: size,
    dbSizeMB: (size / 1024 / 1024).toFixed(2),
    dbPath: DB_PATH,
    retentionDays: RETENTION_DAYS,
    pipelineLogLevel: getPipelineLogLevel()
  };
}

/**
 * 获取日志总数
 */
export function getLogCount(options = {}) {
  flushBuffer();

  const { model, success, projectId, startTime, endTime } = options;
  const database = getDb();
  const conditions = ['usage_only = 0'];
  const params = {};

  if (model) {
    conditions.push('model = @model');
    params.model = model;
  }

  if (success !== undefined) {
    conditions.push('success = @success');
    params.success = success ? 1 : 0;
  }

  if (projectId) {
    conditions.push('project_id = @project_id');
    params.project_id = projectId;
  }

  if (startTime) {
    conditions.push('timestamp >= @start_time');
    params.start_time = startTime;
  }

  if (endTime) {
    conditions.push('timestamp <= @end_time');
    params.end_time = endTime;
  }

  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
  const query = `SELECT COUNT(*) as count FROM logs ${whereClause}`;

  const stmt = database.prepare(query);
  const row = stmt.get(params);

  return row?.count || 0;
}

/**
 * 关闭数据库连接
 */
export function closeDb() {
  flushBuffer();
  if (db) {
    db.close();
    db = null;
  }
}

// 进程退出时确保数据写入
process.on('exit', () => {
  flushBuffer();
  closeDb();
});

process.on('SIGINT', () => {
  flushBuffer();
  closeDb();
  process.exit(0);
});

process.on('SIGTERM', () => {
  flushBuffer();
  closeDb();
  process.exit(0);
});

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
 * 注册日志监听器（用于实时广播）
 *
 * @param {Function} callback - 回调函数
 * @returns {Function} 取消注册函数
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
  appendLog,
  getRecentLogs,
  getLogDetail,
  getUsageCountsWithinWindow,
  getUsageCountSince,
  getRecentTokenStats,
  getUsageSummary,
  cleanupOldLogs,
  clearLogs,
  getDbStats,
  getLogCount,
  closeDb,
  onLogAppended
};
