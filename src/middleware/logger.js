/**
 * 请求日志中间件模块 (Request Logger Middleware)
 *
 * 职责：
 * - 记录所有 HTTP 请求的基本信息
 * - 跳过静态资源请求（图片、favicon）
 * - 提取并记录客户端真实 IP
 * - 计算并记录请求响应时间
 *
 * 设计说明：
 * - 使用 res.on('finish') 确保在响应完成后记录
 * - 支持代理环境下的真实 IP 提取
 * - 与业务逻辑解耦，可独立配置
 *
 * @module middleware/logger
 */

import appLogger from '../utils/logger.js';
import { extractClientIP } from '../utils/httpUtils.js';

/**
 * 需要跳过日志记录的路径前缀
 * @constant {string[]}
 */
const SKIP_PATHS = ['/images', '/favicon.ico'];

/**
 * 检查路径是否应跳过日志记录
 *
 * @param {string} path - 请求路径
 * @returns {boolean} 是否跳过
 */
function shouldSkipLogging(path) {
  return SKIP_PATHS.some(prefix => path.startsWith(prefix));
}

/**
 * 请求日志中间件
 *
 * 记录每个请求的方法、路径、状态码、响应时间、客户端 IP 和 User-Agent。
 * 自动跳过静态资源请求。
 *
 * @param {import('express').Request} req
 * @param {import('express').Response} res
 * @param {import('express').NextFunction} next
 *
 * @example
 * app.use(requestLogger);
 * // 日志输出: GET /v1/models 200 123ms 192.168.1.1 Mozilla/5.0...
 */
export function requestLogger(req, res, next) {
  // 跳过静态资源
  if (shouldSkipLogging(req.path)) {
    return next();
  }

  const start = Date.now();

  // 响应完成时记录日志
  res.on('finish', () => {
    const duration = Date.now() - start;
    const clientIP = extractClientIP(req);
    const userAgent = req.headers['user-agent'] || '';

    appLogger.request(
      req.method,
      req.path,
      res.statusCode,
      duration,
      clientIP,
      userAgent
    );
  });

  next();
}

/**
 * 创建可配置的请求日志中间件
 *
 * @param {Object} options - 配置选项
 * @param {string[]} [options.skipPaths] - 要跳过的路径前缀
 * @param {boolean} [options.logBody=false] - 是否记录请求体
 * @returns {Function} Express 中间件
 *
 * @example
 * app.use(createRequestLogger({
 *   skipPaths: ['/health', '/metrics'],
 *   logBody: true
 * }));
 */
export function createRequestLogger(options = {}) {
  const skipPaths = options.skipPaths || SKIP_PATHS;
  const logBody = options.logBody || false;

  return (req, res, next) => {
    // 检查是否跳过
    if (skipPaths.some(prefix => req.path.startsWith(prefix))) {
      return next();
    }

    const start = Date.now();

    res.on('finish', () => {
      const duration = Date.now() - start;
      const clientIP = extractClientIP(req);
      const userAgent = req.headers['user-agent'] || '';

      appLogger.request(
        req.method,
        req.path,
        res.statusCode,
        duration,
        clientIP,
        userAgent
      );

      // 可选：记录请求体（用于调试）
      if (logBody && req.body && Object.keys(req.body).length > 0) {
        appLogger.debug(`Request body: ${JSON.stringify(req.body).slice(0, 500)}`);
      }
    });

    next();
  };
}

export default requestLogger;
