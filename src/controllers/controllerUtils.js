/**
 * 控制器工具函数 (Controller Utilities)
 *
 * 职责：
 * - 提供控制器共享的工具函数
 * - 模型列表查询
 * - 凭证使用量统计
 * - Gemini 响应图片处理
 * - 模型别名解析
 * - 日志写入工具
 *
 * @module controllers/controllerUtils
 */

import logger from '../utils/logger.js';
import tokenManager from '../auth/token_manager.js';
import { saveBase64Image } from '../utils/imageStorage.js';
import { getAvailableModels } from '../api/client.js';
import { getUsageCountsWithinWindow, appendLog } from '../utils/log_store.js';

/**
 * 将 Gemini 响应中的 inlineData 落地为 URL
 *
 * 遍历响应中的所有 candidate parts，将 base64 图片数据
 * 保存为文件并添加可访问的 URL。
 *
 * @param {Object} response - Gemini 原始响应
 * @returns {Object} 处理后的响应（添加了 imageUrl 字段）
 *
 * @example
 * const processed = attachImageUrlsToGeminiResponse(geminiResponse);
 * // processed.candidates[0].content.parts[0].imageUrl = '/images/xxx.png'
 */
export function attachImageUrlsToGeminiResponse(response) {
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

/**
 * 获取可用模型列表
 *
 * 返回系统支持的所有模型信息。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export async function getModels(req, res) {
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
}

/**
 * 获取凭证使用量列表
 *
 * 返回每个凭证在过去一小时内的使用次数和剩余额度。
 * 用于客户端负载均衡和监控。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 */
export function getCredentialLimits(req, res) {
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
}

/**
 * OpenAI 图像尺寸到 Gemini imageSize 的映射
 *
 * @constant {Object}
 */
export const IMAGE_SIZE_MAP = {
  '256x256': '1K',
  '512x512': '1K',
  '1024x1024': '1K',
  '1536x1536': '2K',
  '2048x2048': '2K',
  '4096x4096': '4K'
};

/**
 * 解析模型别名后缀
 *
 * 提取模型名称中的分辨率后缀（-1k/-2k/-4k），
 * 返回原始模型名和分辨率参数。
 *
 * @param {string} model - 原始模型名
 * @returns {{ upstreamModel: string, imageSize: string|null }}
 *
 * @example
 * parseModelAlias('gemini-2.0-flash-image-2k')
 * // { upstreamModel: 'gemini-2.0-flash-image', imageSize: '2K' }
 */
export function parseModelAlias(model) {
  if (typeof model !== 'string') {
    return { upstreamModel: model, imageSize: null };
  }

  const match = model.match(/^(.*-image)(?:-(1k|2k|4k))$/i);
  if (match) {
    return {
      upstreamModel: match[1],
      imageSize: match[2].toUpperCase()
    };
  }

  return { upstreamModel: model, imageSize: null };
}

/**
 * 检查是否为图像生成模型
 *
 * @param {string} model - 模型名称
 * @returns {boolean} 是否为图像模型
 */
export function isImageModel(model) {
  return typeof model === 'string' && model.includes('-image');
}

/**
 * 提取错误状态码
 *
 * @param {any} error - 错误对象
 * @param {number} [defaultStatus=500] - 默认状态码
 * @returns {number}
 */
export function extractErrorStatus(error, defaultStatus = 500) {
  const rawStatus = error?.status || error?.statusCode || error?.response?.status || defaultStatus;
  return parseInt(String(rawStatus), 10);
}

/**
 * 生成重试日志消息
 *
 * @param {Error|Object} error
 * @param {number|null} delayMs
 * @returns {string}
 */
export function formatRetryMessage(error, delayMs) {
  if (!delayMs) return error?.message || '请求失败';
  if (error?.code === 'CAPACITY_EXHAUSTED') {
    return `模型暂无容量，等待 ${Math.round(delayMs)}ms 后重试`;
  }
  return `429 限流，等待 ${Math.round(delayMs)}ms 后重试当前凭证`;
}

/**
 * 创建日志写入函数
 *
 * 生成一个绑定了请求上下文的日志写入函数。
 * 支持动态更新 token 和 model，适用于重试场景。
 *
 * @param {Object} context - 请求上下文
 * @param {import('express').Request} context.req - Express 请求
 * @param {import('express').Response} context.res - Express 响应
 * @param {number} context.startedAt - 请求开始时间戳
 * @param {Object} context.requestSnapshot - 请求快照
 * @param {string} [context.correlationId] - 请求关联 ID
 * @param {string} [context.model] - 模型名称（可覆盖 req.body.model）
 * @returns {{ writeLog: Function, setToken: Function, setModel: Function }}
 */
export function createLogWriter(context) {
  const { req, res, startedAt, requestSnapshot, correlationId, model: initialModel } = context;

  // 可变状态：支持在重试过程中更新
  let currentToken = null;
  let currentModel = initialModel || req.body?.model || req.params?.model || 'unknown';

  const writeLog = ({
    success,
    status,
    message,
    isRetry = false,
    retryCount = 0,
    willRetry = false,
    errorPreview = null,
    rawResponse = null,
    responseBody = null,
    responseSummary = null
  }) => {
    const logEntry = {
      timestamp: new Date().toISOString(),
      model: currentModel,
      projectId: currentToken?.projectId || null,
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
          body: responseBody,
          rawBody: rawResponse,
          modelOutput: responseSummary
        }
      }
    };

    // 仅在有值时添加可选字段
    if (correlationId) logEntry.correlationId = correlationId;
    if (isRetry) logEntry.isRetry = isRetry;
    if (retryCount > 0) logEntry.retryCount = retryCount;
    if (willRetry) logEntry.willRetry = willRetry;
    if (errorPreview) logEntry.errorPreview = errorPreview;

    appendLog(logEntry);

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
          body: responseBody,
          modelOutput: responseSummary
        },
        error: success ? undefined : message
      });
    }
  };

  return {
    writeLog,
    setToken: (token) => { currentToken = token; },
    setModel: (model) => { currentModel = model; }
  };
}

// 注意: httpUtils 函数应直接从 '../utils/httpUtils.js' 导入
// 路由模块已直接引用 httpUtils，此处不再重导出

export default {
  attachImageUrlsToGeminiResponse,
  getModels,
  getCredentialLimits,
  IMAGE_SIZE_MAP,
  parseModelAlias,
  isImageModel,
  createLogWriter,
  formatRetryMessage,
  extractErrorStatus
};
