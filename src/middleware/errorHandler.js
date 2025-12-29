/**
 * 错误处理中间件模块 (Error Handler Middleware)
 *
 * 职责：
 * - 处理请求体过大错误（413）
 * - 处理 JSON 解析错误（400）
 * - 处理未捕获的异常（500）
 * - 提供统一的错误响应格式
 *
 * 设计说明：
 * - 错误处理中间件必须放在路由之后
 * - 使用四参数签名 (err, req, res, next) 标识错误处理中间件
 * - 区分开发和生产环境的错误信息暴露程度
 *
 * @module middleware/errorHandler
 */

import config from '../config/config.js';
import logger from '../utils/logger.js';

/**
 * 请求体过大错误处理中间件
 *
 * 处理 express.json() 抛出的 entity.too.large 错误。
 * 返回 413 状态码和友好的错误信息。
 *
 * @param {Error} err - 错误对象
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * app.use(express.json({ limit: '50mb' }));
 * app.use(entityTooLargeHandler);
 */
export function entityTooLargeHandler(err, req, res, next) {
  if (err && err.type === 'entity.too.large') {
    return res.status(413).json({
      error: `Request entity too large, max ${config.security.maxRequestSize}`
    });
  }
  return next(err);
}

/**
 * JSON 解析错误处理中间件
 *
 * 处理 express.json() 抛出的 JSON 语法错误。
 * 返回 400 状态码和解析错误信息。
 *
 * @param {Error} err - 错误对象
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 */
export function jsonParseErrorHandler(err, req, res, next) {
  if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
    logger.warn(`JSON 解析错误: ${err.message}`);
    return res.status(400).json({
      error: 'Invalid JSON in request body',
      message: err.message
    });
  }
  return next(err);
}

/**
 * 全局错误处理中间件
 *
 * 捕获所有未处理的错误，记录日志并返回统一格式的错误响应。
 * 在开发环境下会返回详细的错误堆栈。
 *
 * @param {Error} err - 错误对象
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * // 必须放在所有路由之后
 * app.use(globalErrorHandler);
 */
export function globalErrorHandler(err, req, res, next) {
  // 如果响应已发送，交给默认处理
  if (res.headersSent) {
    return next(err);
  }

  // 记录错误日志
  logger.error(`Unhandled error: ${err.message}`, {
    path: req.path,
    method: req.method,
    stack: err.stack
  });

  // 确定状态码
  const statusCode = err.statusCode || err.status || 500;

  // 构建错误响应
  const errorResponse = {
    error: err.message || 'Internal Server Error'
  };

  // 开发环境下返回堆栈信息
  if (process.env.NODE_ENV === 'development') {
    errorResponse.stack = err.stack;
  }

  res.status(statusCode).json(errorResponse);
}

/**
 * 404 Not Found 处理中间件
 *
 * 处理所有未匹配路由的请求。
 * 应放在所有路由定义之后。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 *
 * @example
 * app.use(notFoundHandler);
 */
export function notFoundHandler(req, res) {
  res.status(404).json({
    error: 'Not Found',
    path: req.path
  });
}

/**
 * 创建组合的错误处理中间件
 *
 * 按顺序应用所有错误处理器。
 *
 * @returns {Array<Function>} 错误处理中间件数组
 *
 * @example
 * app.use(...createErrorHandlers());
 */
export function createErrorHandlers() {
  return [
    entityTooLargeHandler,
    jsonParseErrorHandler,
    globalErrorHandler
  ];
}

export default globalErrorHandler;
