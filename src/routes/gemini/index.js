/**
 * Gemini API 路由 (Gemini Routes)
 *
 * 职责：
 * - 注册 Gemini generateContent 路由
 * - 注册 Gemini streamGenerateContent 路由
 * - 支持 /v1beta 和 /gemini/v1beta 路径前缀
 *
 * @module routes/gemini
 */

import { Router } from 'express';
import {
  handleGeminiGenerateContent,
  handleGeminiStreamGenerateContent
} from '../../controllers/geminiController.js';

const router = Router();

// ===== Gemini API 路由 =====
// /v1beta/models/:model:generateContent
router.post('/models/:model\\:generateContent', handleGeminiGenerateContent);
router.post('/models/:model\\:streamGenerateContent', handleGeminiStreamGenerateContent);

export default router;
