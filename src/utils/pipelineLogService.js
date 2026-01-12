/**
 * Pipeline 日志服务
 *
 * 封装 4 层请求日志记录：
 * 1. 客户端原始请求（OpenAI/Claude 格式）
 * 2. 转换后的 Gemini 请求
 * 3. Antigravity 原始响应
 * 4. 转换后的客户端响应
 *
 * @module utils/pipelineLogService
 */

import crypto from 'crypto';
import { PipelineContext, createPipelineContext, noopPipelineContext } from './pipelineContext.js';
import { appendLog } from './log_store_sqlite.js';
import log from './logger.js';

// 日志级别配置
const LOG_LEVEL = process.env.PIPELINE_LOG_LEVEL || 'full';

/**
 * 判断是否启用 Pipeline 日志
 */
function isEnabled() {
  return LOG_LEVEL !== 'off';
}

/**
 * 创建 Pipeline 日志会话
 *
 * @param {string} requestId - 请求 ID
 * @param {string} protocol - 客户端协议 ('openai' | 'claude')
 * @param {Object} options - 配置选项
 * @returns {PipelineLogSession}
 */
export function createPipelineLogSession(requestId, protocol, options = {}) {
  if (!isEnabled()) {
    return new PipelineLogSession(requestId, protocol, noopPipelineContext, options);
  }

  const ctx = createPipelineContext(requestId);
  ctx.setMetadata('protocol', protocol);
  ctx.setMetadata('model', options.model || 'unknown');

  return new PipelineLogSession(requestId, protocol, ctx, options);
}

/**
 * Pipeline 日志会话
 *
 * 用于跟踪单个请求的完整转换链路
 */
export class PipelineLogSession {
  /**
   * @param {string} requestId - 请求 ID
   * @param {string} protocol - 客户端协议
   * @param {PipelineContext} ctx - Pipeline 上下文
   * @param {Object} options - 配置选项
   */
  constructor(requestId, protocol, ctx, options = {}) {
    this.requestId = requestId;
    this.protocol = protocol;
    this.ctx = ctx;
    this.options = options;
    this.startTime = Date.now();
    this.model = options.model || 'unknown';
    this.projectId = options.projectId || null;
    this.correlationId = options.correlationId || null;

    // 存储各阶段数据（用于最终日志）
    this._clientRequest = null;
    this._geminiRequest = null;
    this._antigravityResponse = null;
    this._clientResponse = null;
    this._error = null;
  }

  /**
   * 记录客户端原始请求
   *
   * @param {Object} requestBody - 原始请求体（OpenAI 或 Claude 格式）
   */
  logClientRequest(requestBody) {
    this._clientRequest = requestBody;
    this.ctx.addStage('client-request', null, requestBody, {
      protocol: this.protocol,
      model: requestBody?.model || this.model
    });

    if (LOG_LEVEL === 'full') {
      log.debug(`[Pipeline:${this.requestId?.slice(0, 8)}] Client Request (${this.protocol})`, {
        model: requestBody?.model,
        messageCount: requestBody?.messages?.length
      });
    }
  }

  /**
   * 记录转换后的 Gemini 请求
   *
   * @param {Object} geminiRequest - Gemini 格式请求体
   * @param {Object} wrappedRequest - 包装后的完整请求（含 project, requestId 等）
   */
  logGeminiRequest(geminiRequest, wrappedRequest = null) {
    // 打破循环引用：移除 _pipelineSession 避免 JSON.stringify 崩溃
    if (wrappedRequest) {
      const { _pipelineSession, ...safeWrapped } = wrappedRequest;
      this._geminiRequest = safeWrapped;
    } else {
      this._geminiRequest = geminiRequest;
    }

    this.ctx.addStage('gemini-request', this._clientRequest, geminiRequest, {
      contentsCount: geminiRequest?.contents?.length,
      hasTools: !!(geminiRequest?.tools?.length),
      thinkingConfig: geminiRequest?.generationConfig?.thinkingConfig
    });

    if (LOG_LEVEL === 'full') {
      log.debug(`[Pipeline:${this.requestId?.slice(0, 8)}] Gemini Request`, {
        contentsCount: geminiRequest?.contents?.length,
        hasTools: !!(geminiRequest?.tools?.length),
        thinkingBudget: geminiRequest?.generationConfig?.thinkingConfig?.thinkingBudget
      });
    }
  }

  /**
   * 记录 Antigravity 原始响应
   *
   * @param {Object|string} response - Antigravity 响应（可能是 JSON 或字符串）
   * @param {Object} metadata - 额外元数据（status, durationMs 等）
   */
  logAntigravityResponse(response, metadata = {}) {
    this._antigravityResponse = response;
    this.ctx.addStage('antigravity-response', null, response, {
      status: metadata.status,
      durationMs: metadata.durationMs,
      hasError: metadata.hasError || false
    });

    if (LOG_LEVEL === 'full') {
      log.debug(`[Pipeline:${this.requestId?.slice(0, 8)}] Antigravity Response`, {
        status: metadata.status,
        durationMs: metadata.durationMs
      });
    }
  }

  /**
   * 记录转换后的客户端响应
   *
   * @param {Object} response - 客户端格式响应
   * @param {Object} metadata - 额外元数据
   */
  logClientResponse(response, metadata = {}) {
    this._clientResponse = response;
    this.ctx.addStage('client-response', this._antigravityResponse, response, {
      protocol: this.protocol,
      finishReason: metadata.finishReason,
      hasToolCalls: metadata.hasToolCalls
    });

    if (LOG_LEVEL === 'full') {
      log.debug(`[Pipeline:${this.requestId?.slice(0, 8)}] Client Response (${this.protocol})`, {
        finishReason: metadata.finishReason
      });
    }
  }

  /**
   * 记录错误
   *
   * @param {string} stage - 发生错误的阶段
   * @param {Error} error - 错误对象
   * @param {Object} context - 错误上下文
   */
  logError(stage, error, context = null) {
    this._error = error;
    this.ctx.addError(stage, error, context);

    log.error(`[Pipeline:${this.requestId?.slice(0, 8)}] Error at ${stage}:`, error?.message || error);
  }

  /**
   * 完成会话并写入日志
   *
   * @param {Object} options - 完成选项
   * @returns {Object|null} 写入的日志条目
   */
  finish(options = {}) {
    const durationMs = Date.now() - this.startTime;
    const hasError = this.ctx.hasErrors() || options.success === false;

    // 使用 ctx 的 sanitize 方法清理数据（如果可用）
    const sanitize = typeof this.ctx?.sanitize === 'function'
      ? (value) => this.ctx.sanitize(value)
      : (value) => value;

    // 构建详情对象（清理后）
    const detail = sanitize({
      clientRequest: this._clientRequest,
      geminiRequest: this._geminiRequest,
      antigravityResponse: this._antigravityResponse,
      clientResponse: this._clientResponse
    });

    // 写入 SQLite（容错处理）
    let logEntry = null;
    try {
      logEntry = appendLog({
        model: this.model,
        projectId: this.projectId,
        correlationId: this.correlationId,
        success: !hasError,
        status: options.status || (hasError ? 500 : 200),
        durationMs,
        path: options.path || `/v1/${this.protocol === 'openai' ? 'chat/completions' : 'messages'}`,
        method: 'POST',
        message: options.message || (hasError ? this._error?.message : 'OK'),
        pipeline: this.ctx.toLogEntry(),
        detail
      });
    } catch (error) {
      log.error(`[Pipeline:${this.requestId?.slice(0, 8)}] Failed to append pipeline log:`, error?.message || error);
    }

    // 输出摘要
    if (LOG_LEVEL !== 'off') {
      log.info(this.ctx.getSummary());
    }

    return logEntry;
  }

  /**
   * 获取 Pipeline Context（用于传递给下游）
   */
  getContext() {
    return this.ctx;
  }
}

/**
 * 从请求创建日志会话的快捷方法
 *
 * @param {import('express').Request} req - Express 请求
 * @param {string} protocol - 协议类型
 * @returns {PipelineLogSession}
 */
export function createSessionFromRequest(req, protocol) {
  const requestId = req.headers['x-request-id'] || req.body?.requestId || crypto.randomUUID();
  const correlationId = req.headers['x-correlation-id'] || requestId;

  return createPipelineLogSession(requestId, protocol, {
    model: req.body?.model,
    correlationId,
    path: req.path,
    method: req.method
  });
}

export default {
  createPipelineLogSession,
  createSessionFromRequest,
  PipelineLogSession
};
