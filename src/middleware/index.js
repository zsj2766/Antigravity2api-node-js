/**
 * 中间件模块统一入口
 *
 * 提供所有中间件的统一导出，简化引用。
 *
 * @module middleware
 */

// 认证中间件
export {
  isProtectedApiPath,
  extractApiKeyFromHeaders,
  validateApiKey,
  requireApiKey,
  createApiKeyGuard,
  getPanelUser,
  isPanelPasswordConfigured,
  getSessionTokenFromReq,
  isPanelAuthed,
  requirePanelAuthPage,
  requirePanelAuthApi,
  validatePanelCredentials
} from './auth.js';

// 日志中间件
export {
  requestLogger,
  createRequestLogger
} from './logger.js';

// 错误处理中间件
export {
  entityTooLargeHandler,
  jsonParseErrorHandler,
  globalErrorHandler,
  notFoundHandler,
  createErrorHandlers
} from './errorHandler.js';
