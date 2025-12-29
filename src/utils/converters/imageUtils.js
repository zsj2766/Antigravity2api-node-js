/**
 * 图片和文档转换工具
 * 统一处理 base64/url → inlineData/fileData
 * 
 * 支持类型：
 * - image (base64/url)
 * - document (base64/url) - PDF, CSV 等
 */

const BASE64_IMAGE_REGEX = /^data:image\/([\w+-]+);base64,(.+)$/;
const BASE64_DATA_REGEX = /^data:([^;]+);base64,(.+)$/;

// 支持的文档 MIME 类型
const DOCUMENT_MIME_TYPES = [
  'application/pdf',
  'text/csv',
  'text/plain',
  'application/json',
  'text/html',
  'text/xml',
  'application/xml'
];

// 文件扩展名到 MIME 类型的映射
const EXT_TO_MIME = {
  // 图片
  'jpg': 'image/jpeg',
  'jpeg': 'image/jpeg',
  'png': 'image/png',
  'gif': 'image/gif',
  'webp': 'image/webp',
  'svg': 'image/svg+xml',
  // 文档
  'pdf': 'application/pdf',
  'csv': 'text/csv',
  'txt': 'text/plain',
  'json': 'application/json',
  'html': 'text/html',
  'xml': 'application/xml'
};

/**
 * 根据文件名推断 MIME 类型
 */
function guessMimeTypeFromFilename(filename) {
  if (!filename) return 'application/octet-stream';
  const ext = filename.split('.').pop()?.toLowerCase();
  return EXT_TO_MIME[ext] || 'application/octet-stream';
}

/**
 * 从 OpenAI 消息内容中提取文本和图片
 * 支持多种图片格式：OpenAI image_url、Claude image source
 */
function extractImagesFromContent(content) {
  const result = { text: '', images: [], documents: [] };

  // 如果content是字符串，直接返回
  if (typeof content === 'string') {
    result.text = content;
    return result;
  }

  // 如果content是数组（multimodal格式）
  if (Array.isArray(content)) {
    for (const item of content) {
      if (item.type === 'text') {
        result.text += item.text || '';
      } else if (item.type === 'image_url') {
        // 提取base64图片或文档数据
        const imageUrl = item.image_url?.url || '';

        // 匹配 data:{mimeType};base64,{data} 格式（支持图片和PDF）
        const match = imageUrl.match(BASE64_DATA_REGEX);
        if (match) {
          const mimeType = match[1];
          const base64Data = match[2];

          if (mimeType.startsWith('image/')) {
            result.images.push({
              inlineData: {
                mimeType: mimeType,
                data: base64Data
              }
            });
          } else if (isDocumentMimeType(mimeType)) {
            result.documents.push({
              inlineData: {
                mimeType: mimeType,
                data: base64Data
              }
            });
          }
        } else if (imageUrl.startsWith('http://') || imageUrl.startsWith('https://')) {
          // 普通 URL 图片（需要后端支持 fileData 格式）
          result.images.push({
            fileData: {
              fileUri: imageUrl,
              mimeType: 'image/jpeg' // 默认 JPEG，实际可能需要根据 URL 推断
            }
          });
        }
      } else if (item.type === 'file' && item.file?.file_data) {
        // OpenAI file 格式：{type: "file", file: {filename: "...", file_data: "data:...;base64,..."}}
        const match = item.file.file_data.match(BASE64_DATA_REGEX);
        if (match) {
          const mimeType = match[1];
          const base64Data = match[2];
          if (mimeType.startsWith('image/')) {
            result.images.push({
              inlineData: {
                mimeType: mimeType,
                data: base64Data
              }
            });
          } else if (isDocumentMimeType(mimeType)) {
            result.documents.push({
              inlineData: {
                mimeType: mimeType,
                data: base64Data
              }
            });
          }
        } else if (item.file.file_data.startsWith('http://') || item.file.file_data.startsWith('https://')) {
          // URL 类型的文件
          const mimeType = guessMimeTypeFromFilename(item.file.filename);
          if (mimeType.startsWith('image/')) {
            result.images.push({
              fileData: {
                fileUri: item.file.file_data,
                mimeType: mimeType
              }
            });
          } else if (isDocumentMimeType(mimeType)) {
            result.documents.push({
              fileData: {
                fileUri: item.file.file_data,
                mimeType: mimeType
              }
            });
          }
        }
      }
    }
  }

  return result;
}

/**
 * 从 Claude 图片块中提取 base64 数据
 * 统一处理 Claude API 格式: {type: "image", source: {type: "base64", media_type: "...", data: "..."}}
 * @param {object} block - 图片块对象
 * @returns {{ mediaType: string, data: string } | null}
 */
function extractBase64FromClaudeImage(block) {
  const source = block?.source;
  if (source?.type === 'base64' && source.media_type && source.data) {
    return {
      mediaType: source.media_type,
      data: source.data
    };
  }
  return null;
}

/**
 * 将 Claude 图片块转换为 Gemini inlineData 格式
 * @param {object} block - Claude 图片块
 * @returns {object|null} - Gemini inlineData 或 null
 */
function convertClaudeImageToGemini(block) {
  const imgData = extractBase64FromClaudeImage(block);
  if (imgData) {
    return {
      inlineData: {
        mimeType: imgData.mediaType,
        data: imgData.data
      }
    };
  }
  // 处理 URL 类型
  if (block?.source?.type === 'url' && block.source.url) {
    return {
      fileData: {
        fileUri: block.source.url,
        mimeType: block.source.media_type || 'image/jpeg'
      }
    };
  }
  return null;
}

/**
 * 从 Claude 文档块中提取 base64 数据
 * Claude 格式: {type: "document", source: {type: "base64", media_type: "application/pdf", data: "..."}}
 * @param {object} block - 文档块对象
 * @returns {{ mediaType: string, data: string } | null}
 */
function extractBase64FromClaudeDocument(block) {
  const source = block?.source;
  if (source?.type === 'base64' && source.media_type && source.data) {
    return {
      mediaType: source.media_type,
      data: source.data
    };
  }
  return null;
}

/**
 * 将 Claude 文档块转换为 Gemini inlineData 格式
 * @param {object} block - Claude 文档块
 * @returns {object|null} - Gemini inlineData 或 null
 */
function convertClaudeDocumentToGemini(block) {
  const docData = extractBase64FromClaudeDocument(block);
  if (docData) {
    return {
      inlineData: {
        mimeType: docData.mediaType,
        data: docData.data
      }
    };
  }
  // 处理 URL 类型
  if (block?.source?.type === 'url' && block.source.url) {
    return {
      fileData: {
        fileUri: block.source.url,
        mimeType: block.source.media_type || 'application/pdf'
      }
    };
  }
  return null;
}

/**
 * 将 Claude 图片块转换为 OpenAI image_url 格式
 * @param {object} block - Claude 图片块
 * @returns {object|null} - OpenAI image_url 格式或 null
 */
function convertClaudeImageToOpenAI(block) {
  const source = block?.source;
  if (source?.type === 'base64' && source.media_type && source.data) {
    return {
      type: 'image_url',
      image_url: {
        url: `data:${source.media_type};base64,${source.data}`,
        detail: 'auto'
      }
    };
  }
  if (source?.type === 'url' && source.url) {
    return {
      type: 'image_url',
      image_url: {
        url: source.url,
        detail: 'auto'
      }
    };
  }
  return null;
}

/**
 * 从 tool_result 内容中提取嵌套的图片和文档
 * Claude tool_result 可以嵌套 image 和 document 类型
 * @param {string|Array} content - tool_result 的 content
 * @returns {{ text: string, images: Array, documents: Array }}
 */
function extractMediaFromToolResult(content) {
  const result = { text: '', images: [], documents: [] };
  
  if (typeof content === 'string') {
    result.text = content;
    return result;
  }
  
  if (!Array.isArray(content)) {
    result.text = typeof content === 'object' ? JSON.stringify(content) : String(content ?? '');
    return result;
  }
  
  for (const block of content) {
    if (!block || typeof block !== 'object') continue;
    
    switch (block.type) {
      case 'text':
        result.text += (result.text ? '\n' : '') + (block.text || '');
        break;
      case 'image':
        const geminiImage = convertClaudeImageToGemini(block);
        if (geminiImage) {
          result.images.push(geminiImage);
        }
        break;
      case 'document':
        const geminiDoc = convertClaudeDocumentToGemini(block);
        if (geminiDoc) {
          result.documents.push(geminiDoc);
        }
        break;
    }
  }
  
  return result;
}

/**
 * 检查 MIME 类型是否为文档类型
 * @param {string} mimeType 
 * @returns {boolean}
 */
function isDocumentMimeType(mimeType) {
  return DOCUMENT_MIME_TYPES.includes(mimeType);
}

export {
  BASE64_IMAGE_REGEX,
  BASE64_DATA_REGEX,
  DOCUMENT_MIME_TYPES,
  extractImagesFromContent,
  extractBase64FromClaudeImage,
  extractBase64FromClaudeDocument,
  convertClaudeImageToGemini,
  convertClaudeDocumentToGemini,
  convertClaudeImageToOpenAI,
  extractMediaFromToolResult,
  isDocumentMimeType
};
