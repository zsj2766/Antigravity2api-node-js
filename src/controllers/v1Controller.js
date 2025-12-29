/**
 * OpenAI 兼容 API 控制器 (v1 Controller)
 *
 * 职责：
 * - 处理 /v1/chat/completions 聊天完成
 * - 处理 /v1/images/generations 图像生成
 * - 处理 /v1/messages Claude 兼容 API
 * - 处理 /v1/messages/count_tokens Token 计数
 *
 * @module controllers/v1Controller
 */

import crypto from 'crypto';
import config from '../config/config.js';
import logger from '../utils/logger.js';
import tokenManager from '../auth/token_manager.js';
import {
  generateAssistantResponse,
  generateAssistantResponseNoStream
} from '../api/client.js';
import {
  generateRequestBody,
  generateRequestBodyFromAnthropic,
  ClaudeSseEmitter,
  countClaudeTokens,
  buildClaudeContentBlocks,
  estimateTokensFromText,
  mapOpenAIFinishToClaude
} from '../utils/converters/index.js';
import {
  createRequestSnapshot,
  summarizeStreamEvents,
  setStreamHeaders,
  createResponseMeta,
  createStreamChunk,
  writeStreamData,
  endStream,
  calculateRetryDelay
} from '../utils/httpUtils.js';
import {
  parseModelAlias,
  isImageModel,
  IMAGE_SIZE_MAP,
  createLogWriter
} from './chatController.js';

/**
 * 提取错误状态码
 *
 * @param {Error} error - 错误对象
 * @param {number} [defaultStatus=500] - 默认状态码
 * @returns {number} 状态码
 */
function extractErrorStatus(error, defaultStatus = 500) {
  const rawStatus = error?.status || error?.statusCode || error?.response?.status || defaultStatus;
  return parseInt(String(rawStatus), 10);
}

/**
 * 检查用户是否指定了图片尺寸参数
 *
 * @param {Object} params - 请求参数
 * @returns {boolean}
 */
function userHasImageSizeParam(params) {
  return !!(
    params.image_size ||
    params.imageSize ||
    params?.generation_config?.image_size ||
    params?.generation_config?.imageSize ||
    params?.generation_config?.image_config?.image_size ||
    params?.generation_config?.image_config?.imageSize ||
    params?.generationConfig?.image_size ||
    params?.generationConfig?.imageSize ||
    params?.generationConfig?.image_config?.image_size ||
    params?.generationConfig?.image_config?.imageSize
  );
}

/**
 * 配置图片模型请求体
 *
 * @param {Object} requestBody - 请求体
 * @param {Object} params - 用户参数
 */
function configureImageModelRequest(requestBody, params) {
  const userGenerationConfig = params.generation_config || params.generationConfig || {};
  const userImageConfig =
    params.image_config ||
    params.imageConfig ||
    userGenerationConfig.image_config ||
    userGenerationConfig.imageConfig ||
    {};

  const aspectRatio =
    params.aspect_ratio ||
    params.aspectRatio ||
    userImageConfig.aspect_ratio ||
    userImageConfig.aspectRatio;
  const imageSize =
    params.image_size ||
    params.imageSize ||
    userImageConfig.image_size ||
    userImageConfig.imageSize;
  const responseModalities =
    params.response_modalities ||
    params.responseModalities ||
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

/**
 * 创建聊天完成处理器工厂函数
 *
 * 支持流式和非流式响应，实现 429 重试策略和凭证轮换机制。
 *
 * @param {Function} resolveToken - Token 解析函数
 * @param {Object} options - 配置选项
 * @returns {Function} Express 请求处理器
 */
export const createChatCompletionHandler = (resolveToken, options = {}) => async (req, res) => {
  const { messages, model, stream = true, tools, tool_choice, ...params } = req.body || {};
  const startedAt = Date.now();
  const correlationId = req.headers['x-correlation-id'] || req.headers['x-request-id'] || crypto.randomUUID();
  const requestSnapshot = createRequestSnapshot(req);

  // 使用统一的日志工具
  const { writeLog, setToken } = createLogWriter({
    req, res, startedAt, requestSnapshot, correlationId, model
  });

  let streamEventsForLog = [];
  let responseBodyForLog = null;
  let responseSummaryForLog = null;

  if (!messages) {
    res.status(400).json({ error: 'messages is required' });
    writeLog({ success: false, status: 400, message: 'messages is required' });
    return;
  }

  const maxAttempts = config.retry?.maxAttempts || 3;
  const retryStatusCodes = config.retry?.statusCodes || [429, 500];
  let attempt = 0;
  let retryCountForLog = 0;
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

    try {
      if (res.writableEnded || req.destroyed) break;

      // 获取 token
      let token;
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

      setToken(token);

      // 解析模型别名
      const { upstreamModel, imageSize: imageSizeFromModel } = parseModelAlias(model);

      // 将分辨率写入参数（仅当用户未显式传入时）
      const paramsWithImageSize = { ...params };
      if (imageSizeFromModel && !userHasImageSizeParam(params)) {
        paramsWithImageSize.image_size = imageSizeFromModel;
      }

      const isImgModel = isImageModel(upstreamModel);
      const requestBody = generateRequestBody(messages, upstreamModel, paramsWithImageSize, tools, token, tool_choice);

      if (isImgModel) {
        configureImageModelRequest(requestBody, paramsWithImageSize);
      }

      const { id, created } = createResponseMeta();

      if (stream) {
        if (isImgModel) {
          // 图像模型使用流式API，实现思维链实时传输
          const imageUrls = [];
          let hasStarted = false;
          const { usage } = await generateAssistantResponse(requestBody, token, data => {
            if (!res.headersSent) setStreamHeaders(res);

            if (!hasStarted) {
              hasStarted = true;
              writeStreamData(res, createStreamChunk(id, created, model, { role: 'assistant', content: '' }));
            }

            streamEventsForLog.push(data);

            if (data.type === 'thinking') {
              writeStreamData(res, createStreamChunk(id, created, model, { reasoning_content: data.content }));
            } else if (data.type === 'image') {
              imageUrls.push(data.url);
            } else if (data.type === 'text') {
              writeStreamData(res, createStreamChunk(id, created, model, { content: data.content }));
            }
          });

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
          let hasStarted = false;
          const { usage, finishReason } = await generateAssistantResponse(requestBody, token, data => {
            if (!res.headersSent) setStreamHeaders(res);

            if (!hasStarted) {
              hasStarted = true;
              writeStreamData(res, createStreamChunk(id, created, model, { role: 'assistant', content: '' }));
            }

            streamEventsForLog.push(data);

            let delta = {};
            if (data.type === 'tool_call_chunk') {
              delta = {
                tool_calls: [{
                  index: data.index,
                  id: data.tool_call.id,
                  type: 'function',
                  function: data.tool_call.function
                }]
              };
            } else if (data.type === 'tool_calls') {
              delta = {
                tool_calls: (data.tool_calls || []).map((toolCall, index) => ({
                  index,
                  id: toolCall.id,
                  type: toolCall.type,
                  function: toolCall.function
                }))
              };
            } else if (data.type === 'reasoning') {
              delta = {
                type: 'reasoning',
                id: data.id,
                summary: data.summary
              };
            } else if (data.type === 'thinking') {
              const cleanContent = data.content.replace(/^<思考>\n?|\n?<\/思考>$/g, '');
              delta = { reasoning_content: cleanContent };
            } else if (data.type === 'text') {
              const cleanContent = data.content.replace(/<思考>[\s\S]*?<\/思考>/g, '');
              if (cleanContent) {
                delta = { content: cleanContent };
              }
            }

            if (Object.keys(delta).length > 0) {
              if (data.type === 'tool_calls' || data.type === 'tool_call_chunk') hasToolCall = true;
              writeStreamData(res, createStreamChunk(id, created, model, delta));
            }
          });

          if (!res.headersSent) setStreamHeaders(res);
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
        retryCount: retryCountForLog,
        responseBody: responseBodyForLog,
        responseSummary: responseSummaryForLog
      });
      return;

    } catch (error) {
      const errorStatusInt = extractErrorStatus(error);
      const rawResponse = error.rawResponse || null;
      const errorPreview = rawResponse
        ? (typeof rawResponse === 'string' ? rawResponse : JSON.stringify(rawResponse)).slice(0, 500)
        : null;

      // 429 重试策略
      const currentToken = retryingToken || await resolveToken(req, excludedTokenIds).catch(() => null);
      if (currentToken && errorStatusInt === 429) {
        const tokenKey = tokenManager.getTokenKey(currentToken);
        if (!retried429Tokens.has(tokenKey)) {
          const delay = calculateRetryDelay(attempt, error);
          logger.warn(`凭证 ${tokenKey} 遇到 429，等待 ${Math.round(delay)}ms 后重试当前凭证...`);

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
          retryingToken = currentToken;
          attempt--;
          retryCountForLog++;
          continue;
        }
      }

      // 记录失败统计
      if (currentToken) {
        tokenManager.recordFailure(currentToken, errorStatusInt);
        excludedTokenIds.add(tokenManager.getTokenKey(currentToken));
      }

      // NO_TOKEN 错误无法重试
      if (error.code === 'NO_TOKEN') {
        writeLog({ success: false, status: errorStatusInt, message: error.message, errorPreview });
        if (!res.headersSent) {
          res.status(errorStatusInt).json({ error: error.message });
        }
        return;
      }

      // 判断是否可重试
      const isRetryable = retryStatusCodes.includes(errorStatusInt) ||
        error.code === 'TOKEN_DISABLED' ||
        error.code === 'RATE_LIMITED';

      if (!isLastAttempt && isRetryable) {
        logger.warn(`请求失败 (尝试 ${attempt}/${maxAttempts})，正在切换凭证重试: ${error.message}`);
        writeLog({
          success: false,
          status: errorStatusInt,
          message: error.message,
          isRetry: retryCountForLog > 0,
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
      writeLog({
        success: false,
        status: errorStatusInt,
        message: error.message,
        isRetry: retryCountForLog > 0,
        retryCount: retryCountForLog,
        errorPreview,
        rawResponse
      });

      if (!res.headersSent) {
        const { id, created } = createResponseMeta();

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
          writeStreamData(res, createStreamChunk(id, created, model || 'unknown', { role: 'assistant', content: '' }));
          writeStreamData(
            res,
            createStreamChunk(id, created, model || 'unknown', { content: errorContent })
          );
          endStream(res, id, created, model || 'unknown', 'stop');
        } else {
          res.status(errorStatusInt).json({
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

  try {
    if (!prompt || !model) {
      const status = 400;
      const message = 'prompt 和 model 均为必填';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    const imageSize = IMAGE_SIZE_MAP[String(size).toLowerCase()] || null;
    const params = {};
    if (imageSize) params.image_size = imageSize;

    const token = await tokenManager.getToken();
    if (!token) {
      const status = 503;
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(status).json({ error: message });
      writeLog({ success: false, status, message });
      return;
    }

    setToken(token);

    const messages = [{ role: 'user', content: prompt }];
    const requestBody = generateRequestBody(messages, model, params, undefined, token);
    requestBody.requestType = 'image_gen';

    const { content } = await generateAssistantResponseNoStream(requestBody, token);
    const imageUrls = [];
    const urlRegex = /!\[image\]\(([^)]+)\)/g;
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
        return { b64_json: '' };
      }
      return { url };
    });

    const payload = { created, data };
    res.json(payload);
    writeLog({ success: true, status: res.statusCode || 200, responseBody: payload });
  } catch (error) {
    const status = extractErrorStatus(error);
    const message = error?.message || '图片生成失败';
    if (!res.headersSent) {
      res.status(status).json({ error: message });
    }
    writeLog({ success: false, status, message });
  }
}

/**
 * Token 计数处理器
 *
 * @param {import('express').Request} req - Express 请求
 * @param {import('express').Response} res - Express 响应
 */
export function handleCountTokens(req, res) {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);

  const { writeLog } = createLogWriter({ req, res, startedAt, requestSnapshot });

  try {
    const result = countClaudeTokens(req.body || {});
    res.json(result);
    writeLog({ success: true, status: res.statusCode || 200, responseBody: result });
  } catch (error) {
    const status = 400;
    const message = error?.message || '计算失败';
    res.status(status).json({ error: message });
    writeLog({ success: false, status, message });
  }
}

/**
 * Claude Messages API 处理器
 *
 * @param {import('express').Request} req - Express 请求
 * @param {import('express').Response} res - Express 响应
 */
export async function handleClaudeMessages(req, res) {
  const startedAt = Date.now();
  const requestSnapshot = createRequestSnapshot(req);
  const claudeBody = req.body || {};

  const { writeLog, setToken } = createLogWriter({
    req, res, startedAt, requestSnapshot, model: claudeBody.model
  });

  try {
    const tokenStats = (() => {
      try {
        return countClaudeTokens(claudeBody);
      } catch {
        return { input_tokens: 0 };
      }
    })();

    const token = await tokenManager.getToken();
    if (!token) {
      const message = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。';
      res.status(503).json({ error: message });
      writeLog({ success: false, status: 503, message });
      return;
    }

    setToken(token);

    const requestBody = generateRequestBodyFromAnthropic(claudeBody, token);
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
        } else if (data.type === 'tool_call_chunk') {
          hasToolCalls = true;
          await emitter.sendToolCalls([data.tool_call]);
        }
      });

      const stopReason = finishReason
        ? mapOpenAIFinishToClaude(finishReason)
        : (hasToolCalls ? 'tool_use' : 'end_turn');

      emitter.finish(usage, stopReason);
      writeLog({ success: true, status: res.statusCode || 200, responseBody: { stream: true, usage } });
    } else {
      const result = await generateAssistantResponseNoStream(requestBody, token);
      const contentBlocks = buildClaudeContentBlocks(result.content, result.toolCalls);
      const outputTokens =
        result.usage?.completion_tokens ??
        result.usage?.output_tokens ??
        (result.content ? estimateTokensFromText(result.content) : 0);

      const stopReason = result.finishReason
        ? mapOpenAIFinishToClaude(result.finishReason)
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

      res.json(payload);
      writeLog({ success: true, status: res.statusCode || 200, responseBody: payload });
    }
  } catch (error) {
    logger.error('/v1/messages 请求失败:', error?.message || error);
    const status = extractErrorStatus(error);
    if (!res.headersSent) {
      res.status(status).json({ error: error?.message || '服务器失败' });
    }
    writeLog({ success: false, status, message: error?.message });
  }
}

export default {
  createChatCompletionHandler,
  handleImageGeneration,
  handleCountTokens,
  handleClaudeMessages
};
