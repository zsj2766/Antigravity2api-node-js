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
