/**
 * Claude 兼容 API 控制器
 *
 * 职责：
 * - 处理 /v1/messages Claude Messages API
 * - 处理 /v1/messages/count_tokens Token 计数
 *
 * @module controllers/claudeController
 */

import logger from '../utils/logger.js';
import tokenManager from '../auth/token_manager.js';
import {
  generateAssistantResponseNoStream,
  generateAssistantResponseStream
} from '../api/client.js';
import { generateRequestBodyFromAnthropic } from '../bridge/adapter.js';
import { GeminiToClaudeResponseConverter } from '../bridge/res/GeminiToClaudeResponseConverter.js';
import {
  countClaudeTokens,
  buildClaudeContentBlocks,
  estimateTokens,
  mapGeminiStopReason
} from '../bridge/common/index.js';
import {
  createRequestSnapshot,
  setStreamHeaders
} from '../utils/httpUtils.js';
import {
  createLogWriter,
  extractErrorStatus
} from './controllerUtils.js';
import { withRetry } from '../utils/withRetry.js';
import { saveBase64Image } from '../utils/imageStorage.js';

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
 * 处理 Claude 流式响应
 *
 * 使用 try/finally 确保异常时也能正确收尾，避免客户端看不到 message_delta + message_stop 事件
 */
async function handleClaudeStream(requestBody, token, res, requestId, model, inputTokens) {
  setStreamHeaders(res);
  const converter = new GeminiToClaudeResponseConverter();
  const processor = converter.createStreamProcessor(res, {
    requestId,
    model,
    inputTokens,
    imageHandler: saveBase64Image
  });
  processor.emitter.start();

  let lastChunk = null;
  let usage = null;
  let streamError = null;

  try {
    for await (const { chunk, usage: u } of generateAssistantResponseStream(requestBody, token)) {
      lastChunk = chunk;
      if (u) usage = u;
      await processor.process(chunk);
    }
  } catch (error) {
    streamError = error;
    // 不在这里抛出，让 finally 先执行收尾
  } finally {
    // 确保 usage 传递到 finish，避免 lastChunk.usageMetadata 缺失时缓存 token 丢失
    if (lastChunk && usage && !lastChunk.usageMetadata) {
      lastChunk = { ...lastChunk, usageMetadata: usage };
    }

    // 异常时使用 'error' 作为 stop_reason，正常时从 lastChunk 推断
    const stopReason = streamError ? 'error' : undefined;
    processor.finish(lastChunk, stopReason);
  }

  // 收尾完成后再抛出异常，让上层处理
  if (streamError) {
    throw streamError;
  }

  return { usage };
}

/**
 * 处理 Claude 非流式响应
 */
async function handleClaudeNonStream(requestBody, token, res, requestId, model, inputTokens) {
  const result = await generateAssistantResponseNoStream(requestBody, token);

  // 优先使用 Converter 生成的 contentBlocks（保持原始顺序）
  const contentBlocks = Array.isArray(result.contentBlocks) && result.contentBlocks.length > 0
    ? result.contentBlocks
    : (() => {
        const blocks = [];
        if (result.thinking) {
          const thinkingBlock = { type: 'thinking', thinking: result.thinking };
          if (result.thinkingSignature) {
            thinkingBlock.signature = result.thinkingSignature;
          }
          blocks.push(thinkingBlock);
        }
        blocks.push(...buildClaudeContentBlocks(result.content, result.toolCalls));

        if (result.images && result.images.length > 0) {
          for (const img of result.images) {
            if (img.base64) {
              blocks.push({
                type: 'image',
                source: {
                  type: 'base64',
                  media_type: img.mimeType,
                  data: img.base64
                }
              });
            } else {
              blocks.push({
                type: 'text',
                text: `![image](${img.url})`
              });
            }
          }
        }

        // 处理文件数据 (fileData)
        // TODO: Gemini 返回 fileUri (URL)，Claude document 需要 base64，暂降级为文本
        if (result.files && result.files.length > 0) {
          for (const file of result.files) {
            blocks.push({
              type: 'text',
              text: `[File: ${file.mimeType}](${file.url})`
            });
          }
        }

        return blocks;
      })();

  const outputTokens =
    result.usage?.completion_tokens ??
    result.usage?.output_tokens ??
    (result.content ? estimateTokens(result.content) : 0);

  const stopReason = result.finishReason
    ? mapGeminiStopReason(result.finishReason, result.hasToolCalls).claude
    : (result.toolCalls?.length ? 'tool_use' : 'end_turn');

  const payload = {
    id: `msg_${requestId}`,
    type: 'message',
    role: 'assistant',
    model,
    content: contentBlocks,
    stop_reason: stopReason,
    stop_sequence: null,
    usage: {
      input_tokens: inputTokens,
      output_tokens: outputTokens || 0,
      cache_read_input_tokens: result.usage?.cache_read_input_tokens ??
        result.usage?.prompt_tokens_details?.cached_tokens ??
        result.usage?.cachedContentTokenCount ?? 0,
      cache_creation_input_tokens: result.usage?.cache_creation_input_tokens ?? 0
    }
  };

  res.json(payload);
  return { payload, result };
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

  // 预计算 token 统计
  const tokenStats = (() => {
    try {
      return countClaudeTokens(claudeBody);
    } catch {
      return { input_tokens: 0 };
    }
  })();

  const isStream = claudeBody.stream === true;
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
        const requestBody = await generateRequestBodyFromAnthropic(claudeBody, token);
        const requestId = requestBody.requestId;
        const inputTokens = tokenStats?.input_tokens || 0;

        if (isStream) {
          const { usage } = await handleClaudeStream(
            requestBody, token, res, requestId, claudeBody.model, inputTokens
          );
          return { stream: true, usage };
        } else {
          const { payload } = await handleClaudeNonStream(
            requestBody, token, res, requestId, claudeBody.model, inputTokens
          );
          return { stream: false, payload };
        }
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
    const retryCount = error.retryCount || retryCountForLog;

    if (error.code === 'CONNECTION_CLOSED') {
      const status = extractErrorStatus(error);
      writeLog({
        success: false,
        status,
        message: error?.message,
        isRetry: retryCount > 0,
        retryCount
      });
      return;
    }

    logger.error('/v1/messages 请求失败:', error?.message || error);

    const validationErrors = ['messages 不能为空', 'max_tokens 是必填数字', '请求体格式不合法'];
    const isValidationError = validationErrors.some(msg => error?.message?.includes(msg));
    const status = isValidationError ? 400 : extractErrorStatus(error);

    writeLog({
      success: false,
      status,
      message: error?.message,
      isRetry: retryCount > 0,
      retryCount
    });

    if (!res.headersSent) {
      res.status(status).json({ error: error?.message || '服务器失败' });
    }
  }
}

export default {
  handleCountTokens,
  handleClaudeMessages
};
