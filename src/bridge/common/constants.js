/**
 * Bridge 常量定义
 */

// Data URL 正则
export const DATA_URL_REGEX = /^data:([^;]+);base64,(.+)$/;

// 文档 MIME 类型
export const DOCUMENT_MIME_TYPES = [
  'application/pdf', 'text/csv', 'text/plain',
  'application/json', 'text/html', 'text/xml', 'application/xml'
];

// 音频格式映射
export const AUDIO_FORMAT_MIME = {
  wav: 'audio/wav',
  mp3: 'audio/mp3',
  flac: 'audio/flac',
  opus: 'audio/opus',
  pcm16: 'audio/pcm',
  ogg: 'audio/ogg'
};

// 文件扩展名到 MIME 类型映射
export const EXTENSION_MIME_MAP = {
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  png: 'image/png',
  gif: 'image/gif',
  webp: 'image/webp',
  svg: 'image/svg+xml',
  bmp: 'image/bmp',
  ico: 'image/x-icon',
  pdf: 'application/pdf',
  csv: 'text/csv',
  txt: 'text/plain',
  json: 'application/json',
  html: 'text/html',
  xml: 'application/xml'
};

// Antigravity 系统提示词前缀（与 CLIProxyAPI 一致）
// 仅对 claude / gemini-3-pro 模型生效
export const ANTIGRAVITY_SYSTEM_PREFIX =
  'You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.' +
  'You are pair programming with a USER to solve their coding task. The task may require creating a new codebase, modifying or debugging an existing codebase, or simply answering a question.' +
  '**Absolute paths only**' +
  '**Proactiveness**';

/**
 * 默认安全设置（与 CLIProxyAPI 保持一致）
 * 参考: CLIProxyAPI internal/translator/gemini/common/safety.go
 *
 * 所有危害类别设为 OFF/BLOCK_NONE，避免内容被错误拦截
 */
export const DEFAULT_SAFETY_SETTINGS = [
  { category: 'HARM_CATEGORY_HARASSMENT', threshold: 'OFF' },
  { category: 'HARM_CATEGORY_HATE_SPEECH', threshold: 'OFF' },
  { category: 'HARM_CATEGORY_SEXUALLY_EXPLICIT', threshold: 'OFF' },
  { category: 'HARM_CATEGORY_DANGEROUS_CONTENT', threshold: 'OFF' },
  { category: 'HARM_CATEGORY_CIVIC_INTEGRITY', threshold: 'BLOCK_NONE' }
];

/**
 * 附加默认安全设置（如果请求中没有指定）
 * 与 CLIProxyAPI AttachDefaultSafetySettings 功能一致
 *
 * @param {object} requestBody - Gemini 请求体
 * @returns {object} 附加安全设置后的请求体
 */
export function attachDefaultSafetySettings(requestBody) {
  if (!requestBody || typeof requestBody !== 'object') {
    return requestBody;
  }

  // 如果已有安全设置，不覆盖
  if (requestBody.safetySettings && Array.isArray(requestBody.safetySettings) && requestBody.safetySettings.length > 0) {
    return requestBody;
  }

  return {
    ...requestBody,
    safetySettings: DEFAULT_SAFETY_SETTINGS
  };
}
