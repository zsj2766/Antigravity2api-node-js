/**
 * 统一内容块转换器
 *
 * 【请求/响应转换】内容块格式互转
 *
 * 支持内容类型：
 * - text: 纯文本
 * - image: 图片 (base64/url)
 * - document: 文档 (PDF 等)
 * - thinking: 思考内容
 * - tool_use/functionCall: 工具调用
 * - tool_result/functionResponse: 工具结果
 */

import { generateToolUseId, generateToolCallId, generateReasoningId } from '../../idGenerator.js';
import { safeJsonParse, safeJsonStringify } from '../../utils.js';

// Data URL 正则（匹配 base64 编码的数据 URL）
const DATA_URL_REGEX = /^data:([^;]+);base64,(.+)$/;

// ==================== 【响应转换】Gemini → OpenAI ====================

/**
 * 【响应转换】Gemini parts → OpenAI content
 *
 * 转换方向: Gemini parts → OpenAI content
 *
 * @param {Array} parts - Gemini parts 数组
 * @returns {{ content: string|Array, toolCalls: Array }}
 */
export function convertGeminiToOpenAI(parts) {
  if (!Array.isArray(parts)) {
    return { content: '', toolCalls: [] };
  }

  const contentParts = [];
  const toolCalls = [];
  let hasMultimodal = false;

  for (const part of parts) {
    if (!part) continue;

    // 思考内容 → OpenAI reasoning 格式
    if (part.thought === true && part.text) {
      contentParts.push({
        type: 'reasoning',
        id: generateReasoningId(),
        summary: [{ type: 'summary_text', text: part.text }]
      });
      hasMultimodal = true;
    }
    // 普通文本
    else if (part.text !== undefined) {
      contentParts.push({ type: 'text', text: part.text });
    }
    // 函数调用
    else if (part.functionCall) {
      toolCalls.push({
        id: part.functionCall.id || generateToolCallId(),
        type: 'function',
        function: {
          name: part.functionCall.name,
          arguments: JSON.stringify(part.functionCall.args || {})
        }
      });
    }
    // 内联数据（图片）
    else if (part.inlineData && part.inlineData.mimeType?.startsWith('image/')) {
      hasMultimodal = true;
      contentParts.push({
        type: 'image_url',
        image_url: {
          url: `data:${part.inlineData.mimeType};base64,${part.inlineData.data}`,
          detail: 'auto'
        }
      });
    }
    // 文件数据（图片）
    else if (part.fileData && part.fileData.mimeType?.startsWith('image/')) {
      hasMultimodal = true;
      contentParts.push({
        type: 'image_url',
        image_url: {
          url: part.fileData.fileUri,
          detail: 'auto'
        }
      });
    }
    // 文件数据（非图片）- 添加文本占位符避免静默丢失
    else if (part.fileData) {
      const filename = part.fileData.fileUri?.split('/').pop() || 'document';
      const mimeType = part.fileData.mimeType || 'application/octet-stream';
      contentParts.push({
        type: 'text',
        text: `[File: ${filename} (${mimeType})]`
      });
    }
  }

  // 构建最终内容
  let finalContent;
  if (hasMultimodal || contentParts.some(p => p.type !== 'text')) {
    finalContent = contentParts;
  } else if (contentParts.length === 0) {
    finalContent = '';
  } else {
    finalContent = contentParts
      .filter(p => p.type === 'text')
      .map(p => p.text)
      .join('');
  }

  return { content: finalContent, toolCalls };
}

// ==================== 【响应转换】Gemini → Claude ====================

/**
 * 【响应转换】Gemini parts → Claude content blocks
 *
 * 转换方向: Gemini parts → Claude content blocks
 *
 * @param {Array} parts - Gemini parts 数组
 * @returns {Array} - Claude content blocks
 */
export function convertGeminiToClaude(parts) {
  if (!Array.isArray(parts)) {
    return [];
  }

  const blocks = [];

  for (const part of parts) {
    if (!part) continue;

    // 思考内容 → Claude thinking 块
    if (part.thought === true && part.text) {
      blocks.push({
        type: 'thinking',
        thinking: part.text
        // 注意：不伪造 signature
      });
    }
    // 普通文本
    else if (part.text !== undefined) {
      blocks.push({
        type: 'text',
        text: part.text
      });
    }
    // 函数调用 → Claude tool_use 块
    else if (part.functionCall) {
      blocks.push({
        type: 'tool_use',
        id: part.functionCall.id || generateToolUseId(),
        name: part.functionCall.name,
        input: part.functionCall.args || {}
      });
    }
    // 内联数据（图片）→ Claude image 块
    else if (part.inlineData && part.inlineData.mimeType?.startsWith('image/')) {
      blocks.push({
        type: 'image',
        source: {
          type: 'base64',
          media_type: part.inlineData.mimeType,
          data: part.inlineData.data
        }
      });
    }
    // 内联数据（文档）→ Claude document 块
    else if (part.inlineData) {
      blocks.push({
        type: 'document',
        source: {
          type: 'base64',
          media_type: part.inlineData.mimeType,
          data: part.inlineData.data
        }
      });
    }
    // 文件数据
    else if (part.fileData) {
      if (part.fileData.mimeType?.startsWith('image/')) {
        blocks.push({
          type: 'image',
          source: {
            type: 'url',
            url: part.fileData.fileUri
          }
        });
      } else {
        blocks.push({
          type: 'document',
          source: {
            type: 'url',
            url: part.fileData.fileUri,
            media_type: part.fileData.mimeType
          }
        });
      }
    }
  }

  return blocks;
}

// ==================== 【请求转换】Claude → OpenAI ====================

/**
 * 【请求转换】Claude content → OpenAI content
 *
 * 转换方向: Claude content → OpenAI content
 *
 * @param {string|Array} content - Claude 内容
 * @returns {{ content: string|Array, toolCalls: Array }}
 */
export function convertClaudeToOpenAI(content) {
  // 字符串内容直接返回
  if (typeof content === 'string') {
    return { content, toolCalls: [] };
  }

  if (!Array.isArray(content)) {
    return { content: '', toolCalls: [] };
  }

  const parts = [];
  const toolCalls = [];
  let hasMultimodal = false;

  for (const block of content) {
    if (!block || typeof block !== 'object') continue;

    switch (block.type) {
      case 'text':
        if (block.text && block.text.trim()) {
          parts.push({ type: 'text', text: block.text });
        }
        break;

      case 'thinking':
      case 'redacted_thinking':
        // OpenAI 不支持 thinking，忽略
        break;

      case 'tool_use':
        toolCalls.push({
          id: block.id || generateToolCallId(),
          type: 'function',
          function: {
            name: block.name || 'unknown',
            arguments: safeJsonStringify(block.input) || '{}'
          }
        });
        break;

      case 'image':
        hasMultimodal = true;
        const imgSource = block.source;
        if (imgSource?.type === 'base64') {
          parts.push({
            type: 'image_url',
            image_url: {
              url: `data:${imgSource.media_type};base64,${imgSource.data}`,
              detail: 'auto'
            }
          });
        } else if (imgSource?.type === 'url') {
          parts.push({
            type: 'image_url',
            image_url: {
              url: imgSource.url,
              detail: 'auto'
            }
          });
        }
        break;

      case 'document':
        hasMultimodal = true;
        const docSource = block.source;
        if (docSource) {
          const mediaType = docSource.media_type || 'application/pdf';
          const filename = block.title || `document.${mediaType.split('/')[1] || 'pdf'}`;

          if (docSource.type === 'base64' && docSource.data) {
            parts.push({
              type: 'file',
              file: {
                filename,
                file_data: `data:${mediaType};base64,${docSource.data}`
              }
            });
          } else if (docSource.type === 'url' && docSource.url) {
            parts.push({
              type: 'file',
              file: {
                filename,
                file_data: docSource.url
              }
            });
          }
        }
        break;
    }
  }

  // 构建最终内容
  let finalContent;
  if (hasMultimodal) {
    finalContent = parts;
  } else if (parts.length === 0) {
    finalContent = '';
  } else {
    finalContent = parts
      .filter(p => p.type === 'text')
      .map(p => p.text)
      .join('\n');
  }

  return { content: finalContent, toolCalls };
}

// ==================== 【请求转换】OpenAI → Claude ====================

/**
 * 【请求转换】OpenAI content → Claude content blocks
 *
 * 转换方向: OpenAI content → Claude content blocks
 *
 * @param {string|Array} content - OpenAI 内容
 * @returns {Array} - Claude content blocks
 */
export function convertOpenAIToClaude(content) {
  // 字符串内容
  if (typeof content === 'string') {
    return [{ type: 'text', text: content }];
  }

  if (!Array.isArray(content)) {
    return [{ type: 'text', text: '' }];
  }

  const blocks = [];

  for (const part of content) {
    if (!part || typeof part !== 'object') continue;

    switch (part.type) {
      case 'text':
        if (part.text) {
          blocks.push({ type: 'text', text: part.text });
        }
        break;

      case 'image_url':
        const url = part.image_url?.url;
        if (!url) break;

        // 检查是否为 base64 Data URL
        const base64Match = url.match(DATA_URL_REGEX);
        if (base64Match) {
          blocks.push({
            type: 'image',
            source: {
              type: 'base64',
              media_type: base64Match[1],
              data: base64Match[2]
            }
          });
        } else if (url.startsWith('http://') || url.startsWith('https://')) {
          blocks.push({
            type: 'image',
            source: {
              type: 'url',
              url
            }
          });
        }
        break;

      case 'file':
        const file = part.file;
        if (!file) break;

        // file_data 是 Data URL
        if (file.file_data) {
          const fileMatch = file.file_data.match(DATA_URL_REGEX);
          if (fileMatch) {
            blocks.push({
              type: 'document',
              source: {
                type: 'base64',
                media_type: fileMatch[1],
                data: fileMatch[2]
              },
              title: file.filename
            });
          }
        } else if (file.file_id) {
          blocks.push({
            type: 'document',
            source: {
              type: 'file',
              file_id: file.file_id
            },
            title: file.filename
          });
        }
        break;
    }
  }

  return blocks.length > 0 ? blocks : [{ type: 'text', text: '' }];
}

// ==================== 【请求/响应转换】工具调用互转 ====================

/**
 * 【请求转换】OpenAI tool_calls → Claude tool_use blocks
 *
 * 转换方向: OpenAI tool_calls → Claude tool_use
 *
 * @param {Array} toolCalls - OpenAI tool_calls 数组
 * @returns {Array} - Claude tool_use blocks
 */
export function convertToolCallsToClaude(toolCalls) {
  if (!Array.isArray(toolCalls)) return [];

  return toolCalls.map(tc => {
    const args = safeJsonParse(tc?.function?.arguments, {});
    return {
      type: 'tool_use',
      id: tc.id || generateToolUseId(),
      name: tc.function?.name || 'unknown',
      input: args
    };
  });
}

/**
 * 【请求转换】Claude tool_use blocks → OpenAI tool_calls
 *
 * 转换方向: Claude tool_use → OpenAI tool_calls
 *
 * @param {Array} blocks - Claude content blocks
 * @returns {Array} - OpenAI tool_calls
 */
export function convertToolCallsToOpenAI(blocks) {
  if (!Array.isArray(blocks)) return [];

  return blocks
    .filter(b => b && b.type === 'tool_use')
    .map(b => ({
      id: b.id || generateToolCallId(),
      type: 'function',
      function: {
        name: b.name || 'unknown',
        arguments: safeJsonStringify(b.input) || '{}'
      }
    }));
}

// ==================== 导出 ====================

export const ContentConverter = {
  geminiToOpenAI: convertGeminiToOpenAI,
  geminiToClaude: convertGeminiToClaude,
  claudeToOpenAI: convertClaudeToOpenAI,
  openaiToClaude: convertOpenAIToClaude,
  toolCallsToClaude: convertToolCallsToClaude,
  toolCallsToOpenAI: convertToolCallsToOpenAI
};

export default ContentConverter;
