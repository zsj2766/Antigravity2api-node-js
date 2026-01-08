/**
 * Gemini 控制器 (Gemini Controller)
 *
 * 职责：
 * - 处理 Gemini 原生 generateContent 接口
 * - 处理 Gemini 流式 streamGenerateContent 接口
 * - 模型别名解析和图片尺寸处理
 *
 * @module controllers/geminiController
 */

import tokenManager from '../auth/token_manager.js';
import { generateRequestBodyFromGemini } from '../utils/utils.js';
import {
  generateAssistantResponseStream,
  generateGeminiResponseNoStream
} from '../api/client.js';
import {
  createRequestSnapshot,
  setStreamHeaders
} from '../utils/httpUtils.js';
import {
  attachImageUrlsToGeminiResponse,
  parseModelAlias,
  createLogWriter,
  extractErrorStatus
} from './controllerUtils.js';
import { withRetry } from '../utils/withRetry.js';

/**
 * 应用图片尺寸配置到请求体
 *
 * @param {Object} body - 请求体
 * @param {string|null} imageSizeFromModel - 从模型别名解析的尺寸
 * @private
 */
function applyImageSizeToBody(body, imageSizeFromModel) {
  if (!imageSizeFromModel) return;

  const genCfg = body.generationConfig || {};
  const imgCfg = genCfg.imageConfig || {};
  const hasImageSize = imgCfg.imageSize || imgCfg.image_size;

  if (!hasImageSize) {
    imgCfg.imageSize = imageSizeFromModel;
    genCfg.imageConfig = imgCfg;
    body.generationConfig = genCfg;
  }
}

/**
 * Gemini 非流式 generateContent 处理器
 *
 * @param {import('express').Request} req - Express 请求
 * @param {import('express').Response} res - Express 响应
 */
export async function handleGeminiGenerateContent(req, res) {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const model = req.params.model || req.body?.model || 'unknown';

  const { upstreamModel, imageSize: imageSizeFromModel } = parseModelAlias(model);

  const { writeLog, setToken } = createLogWriter({
    req, res, startedAt, requestSnapshot, model
  });

  const body = req.body || {};
  applyImageSizeToBody(body, imageSizeFromModel);

  if (!Array.isArray(body.contents) || body.contents.length === 0) {
    const status = 400;
    const message = 'contents is required for Gemini generateContent';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
    return;
  }

  let retryCountForLog = 0;

  try {
    const { result, retryCount } = await withRetry({
      resolveToken: (req, excludeIds) => tokenManager.getToken(excludeIds),
      req,
      res,
      onTokenChange: setToken,
      onRetry: (attempt, error, willRetry, delay) => {
        const errorStatusInt = extractErrorStatus(error);
        writeLog({
          success: false,
          status: errorStatusInt,
          message: delay ? `429 限流，等待 ${Math.round(delay)}ms 后重试当前凭证` : error.message,
          isRetry: retryCountForLog > 0,
          retryCount: retryCountForLog,
          willRetry
        });
        retryCountForLog++;
      },
      execute: async (token) => {
        const requestBody = generateRequestBodyFromGemini(body, upstreamModel, token);
        const geminiResponse = await generateGeminiResponseNoStream(requestBody, token);
        const responseWithUrls = attachImageUrlsToGeminiResponse(geminiResponse);

        res.json(responseWithUrls);
        return responseWithUrls;
      }
    });

    writeLog({
      success: true,
      status: res.statusCode || 200,
      isRetry: retryCount > 0,
      retryCount,
      responseBody: result
    });

  } catch (error) {
    const status = extractErrorStatus(error);
    const message = error?.message || 'Gemini generateContent 调用失败';
    const retryCount = error.retryCount || retryCountForLog;

    writeLog({
      success: false,
      status,
      message,
      isRetry: retryCount > 0,
      retryCount
    });

    if (!res.headersSent) {
      res.status(status).json({ error: message });
    }
  }
}

/**
 * Gemini 流式 streamGenerateContent 处理器
 *
 * @param {import('express').Request} req - Express 请求
 * @param {import('express').Response} res - Express 响应
 */
export async function handleGeminiStreamGenerateContent(req, res) {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const model = req.params.model || req.body?.model || 'unknown';

  const { upstreamModel, imageSize: imageSizeFromModel } = parseModelAlias(model);

  const { writeLog, setToken } = createLogWriter({
    req, res, startedAt, requestSnapshot, model
  });

  const body = req.body || {};

  if (!Array.isArray(body.contents) || body.contents.length === 0) {
    const status = 400;
    const message = 'contents is required for Gemini streamGenerateContent';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
    return;
  }

  applyImageSizeToBody(body, imageSizeFromModel);

  let retryCountForLog = 0;
  const streamEventsForLog = [];

  try {
    const { result, retryCount } = await withRetry({
      resolveToken: (req, excludeIds) => tokenManager.getToken(excludeIds),
      req,
      res,
      onTokenChange: setToken,
      onRetry: (attempt, error, willRetry, delay) => {
        const errorStatusInt = extractErrorStatus(error);
        writeLog({
          success: false,
          status: errorStatusInt,
          message: delay ? `429 限流，等待 ${Math.round(delay)}ms 后重试当前凭证` : error.message,
          isRetry: retryCountForLog > 0,
          retryCount: retryCountForLog,
          willRetry
        });
        retryCountForLog++;
      },
      execute: async (token) => {
        const requestBody = generateRequestBodyFromGemini(body, upstreamModel, token);

        setStreamHeaders(res);
        res.flushHeaders();

        let usage = null;

        // 直接透传原始 Gemini chunk（Gemini → Gemini 无需格式转换）
        for await (const { chunk, usage: u } of generateAssistantResponseStream(requestBody, token)) {
          streamEventsForLog.push(chunk);
          if (u) usage = u;
          res.write(`data: ${JSON.stringify(chunk)}\n\n`);
        }

        res.write(`data: ${JSON.stringify({ done: true, usage: usage || null })}\n\n`);
        res.end();

        return { stream: true, events: streamEventsForLog, usage };
      }
    });

    writeLog({
      success: true,
      status: 200,
      isRetry: retryCount > 0,
      retryCount,
      responseBody: result
    });

  } catch (error) {
    const status = extractErrorStatus(error);
    const message = error?.message || 'Gemini streamGenerateContent 调用失败';
    const retryCount = error.retryCount || retryCountForLog;

    writeLog({
      success: false,
      status,
      message,
      isRetry: retryCount > 0,
      retryCount
    });

    if (!res.headersSent) {
      res.status(status).json({ error: message });
    } else {
      res.write(`data: ${JSON.stringify({ error: message })}\n\n`);
      res.end();
    }
  }
}

export default {
  handleGeminiGenerateContent,
  handleGeminiStreamGenerateContent
};
