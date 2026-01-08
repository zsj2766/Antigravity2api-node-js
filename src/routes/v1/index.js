/**
 * OpenAI 兼容 API 路由 (v1 Routes)
 *
 * 职责：
 * - /v1/chat/completions - OpenAI 聊天完成
 * - /v1/models - 模型列表
 * - /v1/lits - 凭证使用量
 * - /v1/images/generations - 图像生成
 * - /v1/messages - Claude 兼容 API
 * - /v1/messages/count_tokens - Token 计数
 *
 * @module routes/v1
 */

import { Router } from 'express';
import tokenManager from '../../auth/token_manager.js';
import {
  getModels,
  getCredentialLimits
} from '../../controllers/controllerUtils.js';
import { createChatCompletionHandler } from '../../controllers/openaiController.js';
import { handleImageGeneration } from '../../controllers/imageController.js';
import {
  handleCountTokens,
  handleClaudeMessages
} from '../../controllers/claudeController.js';

const router = Router();

// ===== 路由注册 =====
router.get('/models', getModels);
router.get('/lits', getCredentialLimits);
router.post('/images/generations', handleImageGeneration);
router.post('/chat/completions', createChatCompletionHandler(
  (req, excludeIds) => tokenManager.getToken(excludeIds)
));
router.post('/messages/count_tokens', handleCountTokens);
router.post('/messages', handleClaudeMessages);

export default router;
export { createChatCompletionHandler };
