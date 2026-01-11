/**
 * OpenAI 兼容 API 控制器
 *
 * 职责：
 * - 处理 /v1/chat/completions 聊天完成
 *
 * @module controllers/openaiController
 */

import crypto from 'crypto';
import logger from '../utils/logger.js';
import tokenManager from '../auth/token_manager.js';
import {
  generateAssistantResponseNoStream,
  generateAssistantResponseStream
} from '../api/client.js';
import { generateRequestBody } from '../bridge/adapter.js';
import { GeminiToOpenAIResponseConverter } from '../bridge/res/GeminiToOpenAIResponseConverter.js';
import { mapGeminiStopReason } from '../bridge/common/index.js';
import {
  createRequestSnapshot,
  summarizeStreamEvents,
  setStreamHeaders,
  createResponseMeta,
  createStreamChunk,
  writeStreamData,
  endStream
} from '../utils/httpUtils.js';
import {
  parseModelAlias,
  createLogWriter,
  formatRetryMessage,
  extractErrorStatus
} from './controllerUtils.js';
import { withRetry } from '../utils/withRetry.js';
import { saveBase64Image } from '../utils/imageStorage.js';

/**
 * 处理聊天流式响应
 *
 * 使用 try/finally 确保异常时也能正确收尾，避免客户端看不到完整的 SSE 事件
 */
async function handleChatStream(requestBody, token, res, id, model, streamEventsForLog, includeUsage) {
  // 初始化流状态标志：用于 withRetry 判断是否可重试
  res.locals = res.locals || {};
  res.locals.streamBodySent = false;

  setStreamHeaders(res);

  // headers 发送后，标记流体已开始
  res.locals.streamBodySent = true;
  const converter = new GeminiToOpenAIResponseConverter();
  const processor = converter.createStreamProcessor(res, {
    requestId: id,
    model,
    inputTokens: 0,
    imageHandler: saveBase64Image,
    includeUsage
  });

  let lastChunk = null;
  let usage = null;
  let streamError = null;

  try {
    for await (const { chunk, usage: u } of generateAssistantResponseStream(requestBody, token)) {
      streamEventsForLog.push(chunk);
      lastChunk = chunk;
      if (u) usage = u;
      processor.process(chunk);
    }
  } catch (error) {
    streamError = error;
    // 不在这里抛出，让 finally 先执行收尾
  } finally {
    // 异常时使用 'error' 作为 finish_reason，正常时从 lastChunk 推断
    const finishReason = streamError ? 'error' : undefined;
    if (res.locals?.streamBodySent === true || !streamError) {
      processor.finish(lastChunk, finishReason);
    }
  }

  // 收尾完成后再抛出异常，让上层处理
  if (streamError) {
    throw streamError;
  }

  return { usage, streamEvents: streamEventsForLog };
}

/**
 * 处理聊天非流式响应
 */
async function handleChatNonStream(requestBody, token, res, id, created, model) {
  const result = await generateAssistantResponseNoStream(requestBody, token);

  let finalContent = result.content || '';
  if (result.images && result.images.length > 0) {
    const imageMarkdown = result.images.map(img => `![image](${img.url})`).join('\n\n');
    finalContent = finalContent ? `${finalContent}\n\n${imageMarkdown}` : imageMarkdown;
  }
  if (result.files && result.files.length > 0) {
    const fileMarkdown = result.files.map(file => `[File: ${file.mimeType}](${file.url})`).join('\n\n');
    finalContent = finalContent ? `${finalContent}\n\n${fileMarkdown}` : fileMarkdown;
  }

  const message = { role: 'assistant', content: finalContent };
  if (result.toolCalls.length > 0) message.tool_calls = result.toolCalls;

  if (result.thinking) {
    message.reasoning_content = result.thinking;
    if (result.thinkingSignature) {
      message.reasoning_signature = result.thinkingSignature;
    }
  }

  const finalFinishReason = result.finishReason
    ? mapGeminiStopReason(result.finishReason, result.hasToolCalls).openai
    : (result.toolCalls.length > 0 ? 'tool_calls' : 'stop');

  const payload = {
    id,
    object: 'chat.completion',
    created,
    model,
    choices: [{ index: 0, message, finish_reason: finalFinishReason }],
    usage: result.usage || null
  };

  res.json(payload);
  return { payload, result };
}

/**
 * 发送错误响应（流式或非流式）
 */
function sendErrorResponse(res, error, stream, model, retryCount) {
  const { id, created } = createResponseMeta();
  const errorStatusInt = extractErrorStatus(error);

  let errorContent = `错误: ${error.message}`;
  if (retryCount > 0) {
    errorContent = `请求失败 (已重试 ${retryCount} 次): ${error.message}`;
  }
  if (error.code === 'RATE_LIMITED' && error.retryAfter) {
    errorContent = `请求被限流，请等待 ${Math.ceil(error.retryAfter / 1000)} 秒后重试。`;
  } else if (error.code === 'CAPACITY_EXHAUSTED') {
    const waitText = error.retryAfter
      ? `请等待 ${Math.ceil(error.retryAfter / 1000)} 秒后重试。`
      : '请稍后重试。';
    errorContent = `模型暂无容量，${waitText}`;
  } else if (error.code === 'TOKEN_DISABLED') {
    errorContent = `凭证已失效或无权限，已自动切换。请重试。`;
  }

  if (stream) {
    setStreamHeaders(res);

  // headers 发送后，标记流体已开始
  res.locals.streamBodySent = true;
    writeStreamData(res, createStreamChunk(id, created, model || 'unknown', { role: 'assistant', content: '' }));
    writeStreamData(res, createStreamChunk(id, created, model || 'unknown', { content: errorContent }));
    endStream(res, id, created, model || 'unknown', 'stop');
  } else {
    res.status(errorStatusInt).json({
      id,
      object: 'chat.completion',
      created,
      model: model || 'unknown',
      choices: [{ index: 0, message: { role: 'assistant', content: errorContent }, finish_reason: 'stop' }],
      error: {
        code: error.code || 'UNKNOWN_ERROR',
        message: error.message,
        retry_after: error.retryAfter ? Math.ceil(error.retryAfter / 1000) : undefined
      }
    });
  }
}

/**
 * 创建聊天完成处理器工厂函数
 *
 * @param {Function} resolveToken - Token 解析函数
 * @param {Object} options - 配置选项
 * @returns {Function} Express 请求处理器
 */
export const createChatCompletionHandler = (resolveToken, options = {}) => async (req, res) => {
  const { messages, model, stream = true, tools, tool_choice, ...params } = req.body || {};
  const includeUsage = req.body?.stream_options?.include_usage === true;
  const startedAt = Date.now();
  const correlationId = req.headers['x-correlation-id'] || req.headers['x-request-id'] || crypto.randomUUID();
  const requestSnapshot = createRequestSnapshot(req);
  if (!res.locals) res.locals = {};
  res.locals.streamMode = stream === true;

  const { writeLog, setToken, logBuilder } = createLogWriter({
    req, res, startedAt, requestSnapshot, correlationId, model
  });

  if (!messages) {
    res.status(400).json({ error: 'messages is required' });
    writeLog({ success: false, status: 400, message: 'messages is required' });
    return;
  }

  let streamEventsForLog = [];
  let retryCountForLog = 0;

  try {
    const { result, retryCount } = await withRetry({
      resolveToken,
      req,
      res,
      tokenMissingError: options.tokenMissingError,
      tokenMissingStatus: options.tokenMissingStatus,
      onTokenChange: setToken,
      onRetry: (attempt, error, willRetry, delay) => {
        const errorStatusInt = extractErrorStatus(error);
        const errorPreview = error.rawResponse
          ? (typeof error.rawResponse === 'string' ? error.rawResponse : JSON.stringify(error.rawResponse)).slice(0, 500)
          : null;
        writeLog({
          success: false,
          status: errorStatusInt,
          message: formatRetryMessage(error, delay),
          isRetry: retryCountForLog > 0,
          retryCount: retryCountForLog,
          willRetry,
          errorPreview
        });
        retryCountForLog++;
      },
      execute: async (token) => {
        streamEventsForLog = [];

        const { upstreamModel } = parseModelAlias(model);

        // 记录转换阶段：OpenAI -> Gemini
        const openaiInput = { messages, model: upstreamModel, tools, tool_choice, ...params };
        const requestBody = await generateRequestBody(messages, upstreamModel, params, tools, token, tool_choice);
        logBuilder.addPipelineStage('openai-to-gemini', openaiInput, requestBody.request);

        // 记录上游请求
        logBuilder.setUpstreamRequest(
          'https://api.antigravity.io/gemini/stream',
          'POST',
          { 'Content-Type': 'application/json' },
          requestBody
        );

        const { id, created } = createResponseMeta();

        if (stream) {
          const { usage, streamEvents } = await handleChatStream(
            requestBody,
            token,
            res,
            id,
            model,
            streamEventsForLog,
            includeUsage
          );
          // 记录上游响应（流式）
          logBuilder.setUpstreamResponse(200, {}, { eventCount: streamEvents.length, usage });
          // 记录转换阶段：Gemini -> OpenAI (流式)
          logBuilder.addPipelineStage('gemini-to-openai-stream', { eventCount: streamEvents.length }, { usage });
          return { stream: true, usage, events: streamEvents, summary: summarizeStreamEvents(streamEvents) };
        } else {
          const { payload, result: chatResult } = await handleChatNonStream(requestBody, token, res, id, created, model);
          // 记录上游响应（非流式）
          logBuilder.setUpstreamResponse(200, {}, chatResult);
          // 记录转换阶段：Gemini -> OpenAI (非流式)
          logBuilder.addPipelineStage('gemini-to-openai', chatResult, payload);
          return {
            stream: false,
            choices: payload.choices,
            usage: chatResult.usage,
            summary: { text: payload.choices[0].message.content, tool_calls: chatResult.toolCalls, usage: chatResult.usage }
          };
        }
      }
    });

    writeLog({
      success: true,
      status: res.statusCode || 200,
      isRetry: retryCount > 0,
      retryCount,
      responseBody: result,
      responseSummary: result.summary
    });

  } catch (error) {
    const errorStatusInt = extractErrorStatus(error);
    const retryCount = error.retryCount || retryCountForLog;

    if (error.code === 'CONNECTION_CLOSED') {
      writeLog({
        success: false,
        status: errorStatusInt,
        message: error.message,
        isRetry: retryCount > 0,
        retryCount
      });
      // 确保响应被正确关闭
      if (!res.writableEnded) {
        res.end();
      }
      return;
    }

    logger.error('生成响应失败:', error.message);
    writeLog({
      success: false,
      status: errorStatusInt,
      message: error.message,
      isRetry: retryCount > 0,
      retryCount
    });

    if (!res.headersSent) {
      if (error.code === 'NO_TOKEN') {
        res.status(errorStatusInt).json({ error: error.message });
      } else {
        sendErrorResponse(res, error, stream, model, retryCount);
      }
    }
  }
};

export default {
  createChatCompletionHandler
};
