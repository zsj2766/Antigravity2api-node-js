/**
 * OpenAI ↔ Gemini 双向转换适配器
 *
 * 职责：
 * 1. 请求转换：OpenAI API 格式 → Antigravity/Gemini 格式
 * 2. 响应转换：Gemini 响应 → OpenAI 格式
 */

import config from '../../config/config.js';
import { generateRequestId, isThinkingModel, generateGenerationConfig } from '../utils.js';
import { generateToolCallId, generateReasoningId } from '../idGenerator.js';
import { extractImagesFromContent, cleanJsonSchema } from './common/index.js';
import {
  getThoughtSignature,
  getTextThoughtSignature,
  safeJsonParse,
  registerTextThoughtSignature,
  registerThoughtSignature
} from '../utils.js';
import { saveBase64Image } from '../imageStorage.js';
import { generateDeterministicToolCallId } from './common/messageUtils.js';

// PDF MIME 类型检测
const PDF_MIME_TYPE = 'application/pdf';
const BASE64_PDF_REGEX = /^data:application\/pdf;base64,(.+)$/;

// ==================== 请求转换：OpenAI → Gemini ====================

/**
 * 处理用户消息，转换为 Antigravity 格式
 */
function handleUserMessage(extracted, antigravityMessages) {
  const parts = [];

  if (extracted.text && extracted.text.trim()) {
    parts.push({ text: extracted.text });
  }

  if (extracted.images && extracted.images.length > 0) {
    parts.push(...extracted.images);
  }

  // 添加文档支持（PDF 等）
  if (extracted.documents && extracted.documents.length > 0) {
    parts.push(...extracted.documents);
  }

  if (parts.length === 0) {
    parts.push({ text: '' });
  }

  antigravityMessages.push({
    role: "user",
    parts
  });
}

/**
 * 处理工具调用结果消息
 */
function handleToolCall(message, antigravityMessages) {
  let functionName = '';
  for (let i = antigravityMessages.length - 1; i >= 0; i--) {
    if (antigravityMessages[i].role === 'model') {
      const parts = antigravityMessages[i].parts;
      for (const part of parts) {
        if (part?.functionCall?.id === message.tool_call_id) {
          functionName = part.functionCall.name;
          break;
        }
      }
      if (functionName) break;
    }
  }

  let output = message.content;
  if (typeof output === 'object' && output !== null) {
    output = output.text || JSON.stringify(output);
  } else if (Array.isArray(output)) {
    const textItem = output.find(item => item?.type === 'text' || typeof item === 'string');
    output = textItem?.text || textItem || JSON.stringify(output);
  }

  const lastMessage = antigravityMessages[antigravityMessages.length - 1];
  const functionResponse = {
    functionResponse: {
      id: message.tool_call_id,
      name: functionName,
      response: {
        output: output
      }
    }
  };

  if (lastMessage?.role === "user" && lastMessage.parts.some(p => p.functionResponse)) {
    lastMessage.parts.push(functionResponse);
  } else {
    antigravityMessages.push({
      role: "user",
      parts: [functionResponse]
    });
  }
}

/**
 * 处理助手消息，支持思维签名
 */
function handleAssistantMessage(message, antigravityMessages, modelName) {
  const lastMessage = antigravityMessages[antigravityMessages.length - 1];
  const hasToolCalls = message.tool_calls && message.tool_calls.length > 0;
  const allowThoughtSignature = typeof modelName === 'string' && modelName.includes('gemini-3');

  let contentText = '';
  let directThoughtSignature = null;

  if (message.content) {
    if (Array.isArray(message.content)) {
      contentText = message.content
        .filter(item => item.type === 'text')
        .map(item => item.text || '')
        .join('');

      const thinkingBlock = message.content.find(item => item.type === 'thinking');
      if (thinkingBlock) {
        directThoughtSignature = thinkingBlock.signature;
      }
    } else if (typeof message.content === 'string') {
      contentText = message.content;
    }
  }
  const hasContent = contentText.trim() !== '';

  const antigravityTools = hasToolCalls ? message.tool_calls.map(toolCall => {
    const args = safeJsonParse(toolCall?.function?.arguments);

    const thoughtSignature = getThoughtSignature(toolCall?.id);
    const part = {
      functionCall: {
        id: toolCall?.id,
        name: toolCall?.function?.name,
        args: args
      }
    };

    if (thoughtSignature) {
      part.thoughtSignature = thoughtSignature;
    }

    return part;
  }) : [];

  if (lastMessage?.role === 'model' && hasToolCalls && !hasContent) {
    lastMessage.parts.push(...antigravityTools);
    return;
  }

  const parts = [];

  if (hasContent) {
    const textThoughtSignature = allowThoughtSignature ? getTextThoughtSignature(contentText) : undefined;
    const finalSignature = directThoughtSignature || textThoughtSignature?.signature;

    const textPart = { text: textThoughtSignature?.text ?? contentText };

    if (allowThoughtSignature && finalSignature) {
      textPart.thoughtSignature = finalSignature;
    }

    parts.push(textPart);
  }

  parts.push(...antigravityTools);

  antigravityMessages.push({
    role: 'model',
    parts
  });
}

/**
 * 将 OpenAI 消息数组转换为 Antigravity 格式
 */
function openaiMessageToAntigravity(openaiMessages, modelName) {
  const antigravityMessages = [];
  for (const message of openaiMessages) {
    if (message.role === "user" || message.role === "system") {
      const extracted = extractImagesFromContent(message.content);
      handleUserMessage(extracted, antigravityMessages);
    } else if (message.role === "assistant") {
      handleAssistantMessage(message, antigravityMessages, modelName);
    } else if (message.role === "tool") {
      handleToolCall(message, antigravityMessages);
    }
  }

  return antigravityMessages;
}

/**
 * 将 OpenAI/Claude 工具定义转换为 Antigravity 格式
 */
function convertOpenAIToolsToAntigravity(tools) {
  if (!tools || !Array.isArray(tools) || tools.length === 0) return [];

  return tools.map((tool) => {
    if (!tool || typeof tool !== 'object') return null;

    const isOpenAIFormat = (tool.type === 'function' || tool.function) && typeof tool.function === 'object';

    const name = isOpenAIFormat ? tool.function.name : tool.name;
    const description = isOpenAIFormat ? tool.function.description : tool.description;
    const rawParameters = isOpenAIFormat
      ? (tool.function?.parameters || {})
      : (tool.input_schema || {});

    const parameters = rawParameters ? JSON.parse(JSON.stringify(rawParameters)) : {};
    const cleanedParameters = cleanJsonSchema(parameters);

    return {
      functionDeclarations: [
        {
          name,
          description,
          parameters: cleanedParameters
        }
      ]
    };
  }).filter(Boolean);
}

/**
 * 生成完整的 Antigravity 请求体
 */
function generateRequestBody(openaiMessages, modelName, parameters, openaiTools, token) {
  const actualModelName = modelName;
  const enableThinking = isThinkingModel(modelName);
  const contents = openaiMessageToAntigravity(openaiMessages, actualModelName);

  return {
    project: token.projectId,
    requestId: generateRequestId(),
    request: {
      contents,
      systemInstruction: {
        role: "user",
        parts: [{ text: config.systemInstruction }]
      },
      tools: convertOpenAIToolsToAntigravity(openaiTools),
      toolConfig: {
        functionCallingConfig: {
          mode: "VALIDATED"
        }
      },
      generationConfig: generateGenerationConfig(parameters, enableThinking, actualModelName),
      sessionId: token.sessionId
    },
    model: actualModelName,
    userAgent: "antigravity"
  };
}

// ==================== 响应转换：Gemini → OpenAI ====================

/**
 * 转换 functionCall 为 OpenAI 格式
 */
function convertGeminiToOpenAIToolCall(functionCall) {
  return {
    id: functionCall.id || generateToolCallId(),
    type: 'function',
    function: {
      name: functionCall.name,
      arguments: JSON.stringify(functionCall.args)
    }
  };
}

/**
 * 在保留原有结构的同时记录 thoughtSignature
 */
function convertToToolCallWithSignature(functionCall, thoughtSignature) {
  const toolCall = convertGeminiToOpenAIToolCall(functionCall);
  if (thoughtSignature && toolCall && toolCall.id) {
    registerThoughtSignature(toolCall.id, thoughtSignature);
  }
  return toolCall;
}

/**
 * 将 Gemini usageMetadata 转换为 OpenAI usage 格式
 */
function toOpenAiUsage(usageMetadata) {
  if (!usageMetadata) return null;

  const prompt = usageMetadata.promptTokenCount ?? usageMetadata.inputTokenCount ?? null;
  const completion = usageMetadata.candidatesTokenCount ?? usageMetadata.outputTokenCount ?? null;
  const total =
    usageMetadata.totalTokenCount ??
    (Number.isFinite(prompt) && Number.isFinite(completion) ? prompt + completion : null);
  const inferredCompletion =
    completion ?? (Number.isFinite(total) && Number.isFinite(prompt) ? Math.max(total - prompt, 0) : total);

  return {
    prompt_tokens: prompt,
    completion_tokens: inferredCompletion,
    total_tokens:
      total ?? (Number.isFinite(prompt) && Number.isFinite(inferredCompletion) ? prompt + inferredCompletion : null)
  };
}

/**
 * 刷新文本累积器，注册 thoughtSignature
 */
function flushTextAccumulator(state) {
  if (!state?.textAccumulator) return;
  const { text, signature } = state.textAccumulator;
  if (text && signature) {
    registerTextThoughtSignature(text, signature);
  }
  state.textAccumulator = { text: '', signature: null };
}

/**
 * 解析 SSE 流式响应片段并通过 callback 发送
 */
function parseGeminiStreamToOpenAI(line, state, callback) {
  if (!line.startsWith('data: ')) return;

  try {
    const data = JSON.parse(line.slice(6));
    const parts = data.response?.candidates?.[0]?.content?.parts;

    if (data.response?.usageMetadata) {
      state.usage = toOpenAiUsage(data.response.usageMetadata);
    }

    if (parts) {
      for (const part of parts) {
        if (part.thought === true) {
          if (part.text) {
            if (!state.reasoningId) {
              state.reasoningId = generateReasoningId();
            }
            callback({
              type: 'reasoning',
              id: state.reasoningId,
              summary: [{ type: 'summary_text', text: part.text }]
            });
          }
        } else if (part.text !== undefined) {
          if (part.thoughtSignature) {
            registerTextThoughtSignature(part.text, part.thoughtSignature);
            state.textAccumulator.signature = part.thoughtSignature;
          }
          state.textAccumulator.text += part.text || '';
          callback({ type: 'text', content: part.text });
        } else if (part.functionCall) {
          state.toolCalls.push(convertToToolCallWithSignature(part.functionCall, part.thoughtSignature));
        } else if (part.inlineData) {
          const imageUrl = saveBase64Image(part.inlineData.data, part.inlineData.mimeType);
          callback({
            type: 'image',
            url: imageUrl,
            mimeType: part.inlineData.mimeType,
            data: part.inlineData.data,
            thought: part.thought === true
          });
        }
      }
    }

    if (data.response?.candidates?.[0]?.finishReason) {
      flushTextAccumulator(state);
      if (state.toolCalls.length > 0) {
        callback({ type: 'tool_calls', tool_calls: state.toolCalls });
        state.toolCalls = [];
      }
    }
  } catch (e) {
    // 忽略 JSON 解析错误
  }
}

export {
  // 请求转换
  generateRequestBody,
  openaiMessageToAntigravity,
  convertOpenAIToolsToAntigravity,
  handleUserMessage,
  handleToolCall,
  handleAssistantMessage,
  // 响应转换
  convertGeminiToOpenAIToolCall,
  convertToToolCallWithSignature,
  toOpenAiUsage,
  flushTextAccumulator,
  parseGeminiStreamToOpenAI,
  // 新增：Gemini → OpenAI 辅助函数
  convertGeminiInlineDataToOpenAI,
  convertGeminiFileDataToOpenAI,
  convertGeminiPartsToOpenAIContent
};

// ==================== Gemini → OpenAI 辅助转换 ====================

/**
 * 将 Gemini fileData 转换为 OpenAI 兼容格式
 * @param {object} fileData - Gemini fileData 对象 {mimeType, fileUri}
 * @returns {object|null} - OpenAI content part
 */
function convertGeminiFileDataToOpenAI(fileData) {
  if (!fileData || !fileData.fileUri) {
    return null;
  }

  const mimeType = (fileData.mimeType || 'unknown').toLowerCase();

  // 图片类型
  if (mimeType.startsWith('image/')) {
    return {
      type: 'image_url',
      image_url: {
        url: fileData.fileUri,
        detail: 'auto'
      }
    };
  }

  // 文档类型 (PDF 等)
  if (mimeType === PDF_MIME_TYPE) {
    return {
      type: 'text',
      text: `[Document: ${mimeType} | ${fileData.fileUri}]`
    };
  }

  // 其他类型
  return {
    type: 'text',
    text: `[Unsupported file: ${mimeType} | ${fileData.fileUri}]`
  };
}

/**
 * 将 Gemini inlineData 转换为 OpenAI image_url 格式
 * @param {object} inlineData - Gemini inlineData 对象 {mimeType, data}
 * @returns {object|null} - OpenAI image_url 格式或 null
 */
function convertGeminiInlineDataToOpenAI(inlineData) {
  if (!inlineData || !inlineData.mimeType || !inlineData.data) {
    return null;
  }

  // 图片类型
  if (inlineData.mimeType.startsWith('image/')) {
    return {
      type: 'image_url',
      image_url: {
        url: `data:${inlineData.mimeType};base64,${inlineData.data}`,
        detail: 'auto'
      }
    };
  }

  // PDF 类型 - OpenAI 不原生支持，转为占位符
  if (inlineData.mimeType === PDF_MIME_TYPE) {
    return {
      type: 'text',
      text: `[Document: ${inlineData.mimeType}]`
    };
  }

  // 其他类型
  return {
    type: 'text',
    text: `[Unsupported content: ${inlineData.mimeType}]`
  };
}

/**
 * 将 Gemini parts 数组转换为 OpenAI content 格式
 * @param {Array} parts - Gemini parts 数组
 * @returns {{ content: string|Array, toolCalls: Array }}
 */
function convertGeminiPartsToOpenAIContent(parts) {
  if (!Array.isArray(parts)) {
    return { content: '', toolCalls: [] };
  }

  const contentParts = [];
  const toolCalls = [];
  let hasMultimodal = false;

  for (const part of parts) {
    if (!part) continue;

    // 思考内容（转为 OpenAI Reasoning 结构）- 必须在普通文本前检查
    if (part.thought === true && part.text) {
      contentParts.push({
        type: 'reasoning',
        id: generateReasoningId(),
        summary: [{ type: 'summary_text', text: part.text }]
      });
    }
    // 文本内容
    else if (part.text !== undefined) {
      contentParts.push({ type: 'text', text: part.text });
    }
    // 内联数据（图片/文档）
    else if (part.inlineData) {
      hasMultimodal = true;
      const converted = convertGeminiInlineDataToOpenAI(part.inlineData);
      if (converted) {
        contentParts.push(converted);
      }
    }
    // 文件数据
    else if (part.fileData) {
      hasMultimodal = true;
      const converted = convertGeminiFileDataToOpenAI(part.fileData);
      if (converted) {
        contentParts.push(converted);
      }
    }
    // 函数调用
    else if (part.functionCall) {
      const id = part.functionCall.id || generateDeterministicToolCallId(
        part.functionCall.name,
        part.functionCall.args
      );
      toolCalls.push({
        id,
        type: 'function',
        function: {
          name: part.functionCall.name,
          arguments: JSON.stringify(part.functionCall.args || {})
        }
      });

      // 记录 thoughtSignature
      if (part.thoughtSignature) {
        registerThoughtSignature(id, part.thoughtSignature);
      }
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
