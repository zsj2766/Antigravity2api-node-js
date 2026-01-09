/**
 * 图像生成控制器
 *
 * 职责：
 * - 处理 /v1/images/generations 图像生成请求
 *
 * @module controllers/imageController
 */

import tokenManager from '../auth/token_manager.js';
import { generateAssistantResponseNoStream } from '../api/client.js';
import { generateRequestBody } from '../bridge/adapter.js';
import { createRequestSnapshot } from '../utils/httpUtils.js';
import { withRetry } from '../utils/withRetry.js';
import {
  IMAGE_SIZE_MAP,
  createLogWriter,
  extractErrorStatus
} from './controllerUtils.js';

/**
 * 图像生成处理器
 *
 * @param {import('express').Request} req - Express 请求
 * @param {import('express').Response} res - Express 响应
 */
export async function handleImageGeneration(req, res) {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const { prompt, model, size, response_format } = req.body || {};

  const { writeLog, setToken } = createLogWriter({
    req, res, startedAt, requestSnapshot, model
  });

  if (!prompt || !model) {
    const status = 400;
    const message = 'prompt 和 model 均为必填';
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
        const imageSize = IMAGE_SIZE_MAP[String(size).toLowerCase()] || null;
        const params = {};
        if (imageSize) params.image_size = imageSize;

        const messages = [{ role: 'user', content: prompt }];
        const requestBody = await generateRequestBody(messages, model, params, undefined, token);
        requestBody.requestType = 'image_gen';

        const apiResult = await generateAssistantResponseNoStream(requestBody, token);

        let imageUrls = [];
        if (apiResult.images && apiResult.images.length > 0) {
          imageUrls = apiResult.images.map(img => img.url);
        } else {
          const urlRegex = /!\[image\]\(([^)]+)\)/g;
          let match;
          while ((match = urlRegex.exec(apiResult.content || '')) !== null) {
            if (match[1]) imageUrls.push(match[1]);
          }
        }

        if (imageUrls.length === 0) {
          const noImageError = new Error('上游未返回图片');
          noImageError.status = 502;
          throw noImageError;
        }

        const created = Math.floor(Date.now() / 1000);
        const data = imageUrls.map(url => {
          if (response_format === 'b64_json') {
            return { b64_json: '' };
          }
          return { url };
        });

        const payload = { created, data };
        res.json(payload);
        return payload;
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
    const message = error?.message || '图片生成失败';
    const retryCount = error.retryCount || retryCountForLog;

    if (error.code === 'CONNECTION_CLOSED') {
      writeLog({
        success: false,
        status,
        message,
        isRetry: retryCount > 0,
        retryCount
      });
      return;
    }

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

export default { handleImageGeneration };
