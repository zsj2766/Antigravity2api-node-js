/**
 * 控制器模块统一入口
 *
 * 提供所有控制器的统一导出，简化引用。
 *
 * @module controllers
 */

// 健康检查控制器
export {
  healthCheck,
  healthCheckDetailed
} from './healthController.js';

// OAuth 控制器
export {
  getOAuthState,
  getOAuthUrl,
  renderOAuthCallback,
  parseOAuthUrl
} from './oauthController.js';

// 管理面板控制器
export {
  // 登录/登出
  renderLoginPage,
  handleLogin,
  handleLogout,
  // 账号管理
  getAccounts,
  refreshAllAccounts,
  getFreezeHistory,
  refreshSingleAccount,
  refreshProjectId,
  deleteAccount,
  toggleAccountEnable,
  importTomlAccounts,
  // 设置管理
  getSettings,
  updateSettings,
  getPanelConfig,
  // 日志管理
  getUsageStats,
  getLogSettings,
  updateLogSettings,
  getLogs,
  clearAllLogs,
  getLogById,
  // 额度查询
  getQuotaList,
  getQuotaAll,
  getTokenStats
} from './adminController.js';

// 控制器工具函数
export {
  attachImageUrlsToGeminiResponse,
  getModels,
  getCredentialLimits,
  IMAGE_SIZE_MAP,
  parseModelAlias,
  isImageModel,
  createLogWriter,
  extractErrorStatus
} from './controllerUtils.js';

// Gemini 控制器
export {
  handleGeminiGenerateContent,
  handleGeminiStreamGenerateContent
} from './geminiController.js';

// OpenAI 兼容 API 控制器
export {
  createChatCompletionHandler
} from './openaiController.js';

// 图像生成控制器
export {
  handleImageGeneration
} from './imageController.js';

// Claude 兼容 API 控制器
export {
  handleCountTokens,
  handleClaudeMessages
} from './claudeController.js';

// 注意: httpUtils 函数应直接从 '../utils/httpUtils.js' 导入
// 不在此处重导出，避免不必要的间接层
