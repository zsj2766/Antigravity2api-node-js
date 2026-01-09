/**
 * 通用重试高阶函数
 *
 * 提供凭证轮换和 429 重试策略的通用实现。
 *
 * @module utils/withRetry
 */

import config from '../config/config.js';
import logger from './logger.js';
import tokenManager from '../auth/token_manager.js';
import { calculateRetryDelay } from './httpUtils.js';
import { extractErrorStatus } from '../controllers/controllerUtils.js';

/**
 * 创建带重试的请求执行器
 *
 * @param {Object} options - 配置选项
 * @param {Function} options.resolveToken - Token 解析函数 (req, excludeIds) => Promise<token>
 * @param {Function} options.execute - 执行函数 (token) => Promise<result>
 * @param {Function} options.onRetry - 重试回调 (attempt, error, willRetry) => void
 * @param {Function} options.onTokenChange - Token 变更回调 (token) => void
 * @param {Object} options.req - Express 请求对象
 * @param {Object} options.res - Express 响应对象
 * @param {string} [options.tokenMissingError] - Token 缺失错误消息
 * @param {number} [options.tokenMissingStatus=503] - Token 缺失状态码
 * @returns {Promise<{ result: any, token: any, retryCount: number }>}
 */
export async function withRetry({
  resolveToken,
  execute,
  onRetry,
  onTokenChange,
  req,
  res,
  tokenMissingError = '没有可用的 token，请先通过 OAuth 面板或 npm run login 获取。',
  tokenMissingStatus = 503
}) {
  const maxAttempts = config.retry?.maxAttempts || 3;
  const retryStatusCodes = config.retry?.statusCodes || [429, 500];

  let attempt = 0;
  let retryCount = 0;
  let lastToken = null;
  const excludedTokenIds = new Set();

  // 429 重试策略状态
  let retryingToken = null;
  const retried429Tokens = new Set();
  let retriedCapacity = false;

  while (attempt < maxAttempts) {
    attempt++;
    const isLastAttempt = attempt === maxAttempts;

    try {
      if (res.writableEnded || req.destroyed) {
        const closedError = new Error('Connection closed');
        closedError.code = 'CONNECTION_CLOSED';
        closedError.status = 499;
        throw closedError;
      }

      // 获取 token
      let token;
      if (retryingToken) {
        token = retryingToken;
        retryingToken = null;
      } else {
        token = await resolveToken(req, excludedTokenIds);
      }

      if (!token) {
        const noTokenError = new Error(tokenMissingError);
        noTokenError.status = tokenMissingStatus;
        noTokenError.code = 'NO_TOKEN';
        throw noTokenError;
      }
      lastToken = token;

      if (onTokenChange) {
        onTokenChange(token);
      }

      // 执行业务逻辑
      const result = await execute(token);

      // 成功：记录统计
      tokenManager.recordSuccess(token);

      return { result, token, retryCount };

    } catch (error) {
      if (error.code === 'CONNECTION_CLOSED') {
        error.retryCount = retryCount;
        throw error;
      }

      // 如果响应头已发送，无法重试，直接抛出
      const canRetryAfterHeaders = res.locals?.streamBodySent === false;
      if (res.headersSent && !canRetryAfterHeaders) {
        error.retryCount = retryCount;
        throw error;
      }

      const errorStatusInt = extractErrorStatus(error);
      const isCapacityExhausted = error.code === 'CAPACITY_EXHAUSTED';

      const currentToken = retryingToken || lastToken || await resolveToken(req, excludedTokenIds).catch(() => null);

      // 容量不足：延迟后重试当前凭证一次（不计入冷却）
      if (isCapacityExhausted && !retriedCapacity) {
        const delay = calculateRetryDelay(attempt, error);
        logger.warn(`模型容量不足，等待 ${Math.round(delay)}ms 后重试...`);

        if (onRetry) {
          onRetry(attempt, error, true, delay);
        }

        await new Promise(resolve => setTimeout(resolve, delay));

        retriedCapacity = true;
        retryCount++;
        retryingToken = currentToken || retryingToken;
        attempt--;
        continue;
      }

      // 429 重试策略：同一凭证重试一次
      if (!isCapacityExhausted && currentToken && errorStatusInt === 429) {
        const tokenKey = tokenManager.getTokenKey(currentToken);
        if (!retried429Tokens.has(tokenKey)) {
          const delay = calculateRetryDelay(attempt, error);
          logger.warn(`凭证 ${tokenKey} 遇到 429，等待 ${Math.round(delay)}ms 后重试当前凭证...`);

          if (onRetry) {
            onRetry(attempt, error, true, delay);
          }

          await new Promise(resolve => setTimeout(resolve, delay));

          retried429Tokens.add(tokenKey);
          retryingToken = currentToken;
          attempt--;
          retryCount++;
          continue;
        }
      }

      // 记录失败统计
      if (currentToken && !isCapacityExhausted) {
        tokenManager.recordFailure(currentToken, errorStatusInt);
        excludedTokenIds.add(tokenManager.getTokenKey(currentToken));
      }

      // NO_TOKEN 错误无法重试
      if (error.code === 'NO_TOKEN') {
        throw error;
      }

      // 判断是否可重试
      const isRetryable = retryStatusCodes.includes(errorStatusInt) ||
        error.code === 'TOKEN_DISABLED' ||
        error.code === 'RATE_LIMITED';

      if (!isLastAttempt && isRetryable) {
        logger.warn(`请求失败 (尝试 ${attempt}/${maxAttempts})，正在切换凭证重试: ${error.message}`);
        if (onRetry) {
          onRetry(attempt, error, true);
        }
        retryCount++;
        continue;
      }

      // 最后一次尝试或不可重试
      error.retryCount = retryCount;
      throw error;
    }
  }
}

export default { withRetry };
