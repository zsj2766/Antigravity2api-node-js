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
  generateAssistantResponse,
  generateGeminiResponseNoStream
} from '../api/client.js';
import {
  createRequestSnapshot,
  setStreamHeaders
} from '../utils/httpUtils.js';
import {
  attachImageUrlsToGeminiResponse,
  parseModelAlias,
  createLogWriter
} from './chatController.js';

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

  try {
    const body = req.body || {};

    applyImageSizeToBody(body, imageSizeFromModel);

    if (!Array.isArray(body.contents) || body.contents.length === 0) {
      const status = 400;
      const message = 'contents is required for Gemini generateContent';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    const token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    setToken(token);

    const requestBody = generateRequestBodyFromGemini(body, upstreamModel, token);
    const geminiResponse = await generateGeminiResponseNoStream(requestBody, token);
    const responseWithUrls = attachImageUrlsToGeminiResponse(geminiResponse);

    res.json(responseWithUrls);
    writeLog({ success: true, status: res.statusCode || 200, responseBody: responseWithUrls });
  } catch (error) {
    const status = 500;
    const message = error?.message || 'Gemini generateContent 调用失败';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
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

  const streamEventsForLog = [];

  try {
    const body = req.body || {};

    if (!Array.isArray(body.contents) || body.contents.length === 0) {
      const status = 400;
      const message = 'contents is required for Gemini streamGenerateContent';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    applyImageSizeToBody(body, imageSizeFromModel);

    const token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    setToken(token);

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
      }
      // tool_calls 在 Gemini 流式中暂不下发
    });

    sendSse({ done: true, usage: usage || null });
    res.end();

    writeLog({
      success: true,
      status: 200,
      responseBody: { stream: true, events: streamEventsForLog, usage }
    });
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
}

export default {
  handleGeminiGenerateContent,
  handleGeminiStreamGenerateContent
};
