/**
 * Gemini → OpenAI 响应转换器
 *
 * 输入: Gemini API 响应格式
 * 输出: OpenAI Chat Completions API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import {
  OpenAIProtocolEmitter,
  generateToolCallId,
  generateRequestId,
  mapGeminiStopReason
} from '../common/index.js';
import { registerTextThoughtSignature, registerThoughtSignature } from '../../utils/utils.js';
import log from '../../utils/logger.js';

export class GeminiToOpenAIResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换 (Gemini → OpenAI)
   *
   * 注意：会注册 thoughtSignature 并映射 finish_reason
   *
   * @param {object} response - Gemini 响应
   * @param {object} context - 上下文
   * @returns {object} OpenAI Chat Completions 响应
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || 'gemini-pro';

    const candidate = response.candidates?.[0];
    if (!candidate) {
      return this.buildEmptyResponse(requestId, model);
    }

    const parts = candidate.content?.parts || [];

    // 注册 thoughtSignature（非流式响应也需要注册，以便后续请求回传）
    for (const part of parts) {
      if (part?.thoughtSignature) {
        if (part.thought === true && part.text) {
          registerTextThoughtSignature(part.text, part.thoughtSignature);
        } else if (part.functionCall?.id) {
          registerThoughtSignature(part.functionCall.id, part.thoughtSignature);
        }
      }
    }

    // 使用内联方法处理多模态内容
    const { content, toolCalls, reasoningContent, reasoningSignature } = this.convertGeminiToOpenAI(parts);

    // 传递 hasToolCalls 参数以正确映射 finish_reason
    const hasToolCalls = toolCalls.length > 0;
    const finishReason = mapGeminiStopReason(candidate.finishReason, hasToolCalls).openai;
    const usage = this.convertUsage(response.usageMetadata);

    const message = {
      role: 'assistant',
      content: content || null
    };

    // 添加 reasoning_content（OpenAI o1/o3 格式）
    if (reasoningContent) {
      message.reasoning_content = reasoningContent;
      // 透传签名 (Scheme B)
      if (reasoningSignature) {
        message.reasoning_signature = reasoningSignature;
      }
    }

    if (toolCalls.length > 0) {
      message.tool_calls = toolCalls;
    }

    return {
      id: `chatcmpl-${requestId}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [{
        index: 0,
        message,
        finish_reason: finishReason
      }],
      usage
    };
  }

  /**
   * 转换响应内容 (Gemini → OpenAI)
   *
   * 注意：内部调用 convertGeminiToOpenAI
   *
   * @param {Array} parts - Gemini parts 数组
   * @returns {object} 提取后的内容结构 { content, toolCalls, reasoningContent }
   */
  convertContent(parts) {
    return this.convertGeminiToOpenAI(parts);
  }

  /**
   * 创建流式响应处理器 (Gemini → OpenAI)
   *
   * 注意：多模态内容在流式中降级为文本占位符
   *
   * @param {object} resOrEmitter - Express response 对象或自定义 Emitter 实例
   * @param {object} context - 上下文（requestId, model 等）
   * @returns {object} 流处理器
   */
  createStreamProcessor(resOrEmitter, context = {}) {
    // 支持注入自定义 Emitter（鸭子类型检查）
    const emitter = (resOrEmitter && typeof resOrEmitter.sendText === 'function' && typeof resOrEmitter.finish === 'function')
      ? resOrEmitter
      : new OpenAIProtocolEmitter(resOrEmitter, {
          requestId: context.requestId || generateRequestId(),
          model: context.model || 'gemini-pro',
          inputTokens: context.inputTokens || 0
        });

    // 追踪是否有工具调用
    let hasToolCalls = false;

    return {
      emitter,

      /**
       * 处理 Gemini 流式 chunk
       */
      process(chunk) {
        const candidate = chunk.candidates?.[0];
        if (!candidate) return;

        const parts = candidate.content?.parts || [];

        // 调试日志：记录 Gemini 原始响应中的 thoughtSignature 情况
        log.info('[GeminiToOpenAI] Received parts:', parts.map(p => ({
          thought: p.thought,
          hasText: !!p.text,
          textPreview: p.text?.substring(0, 50),
          hasSignature: !!p.thoughtSignature,
          signaturePreview: p.thoughtSignature?.substring(0, 30)
        })));

        for (const part of parts) {
          if (!part) continue;

          // 思考内容（Gemini thought: true → OpenAI reasoning_content）
          if (part.thought === true) {
            if (part.text) {
              // 注册 signature 以便后续请求回传
              if (part.thoughtSignature) {
                registerTextThoughtSignature(part.text, part.thoughtSignature);
              }
              // 透传签名 (Scheme B)
              emitter.sendThinking(part.text, part.thoughtSignature);
            } else if (part.thoughtSignature) {
              // 单独的 signature chunk（没有 text）
              emitter.sendThinking('', part.thoughtSignature);
            }
            continue;
          }

          // 普通文本内容
          if (part.text !== undefined) {
            // 注册普通文本的 signature（Gemini 可能在非 thought 文本上也附带 signature）
            if (part.thoughtSignature) {
              registerTextThoughtSignature(part.text, part.thoughtSignature);
            }
            emitter.sendText(part.text);
          }

          // 多模态内容：inlineData (图片/文档)
          if (part.inlineData) {
            const mimeType = part.inlineData.mimeType || 'application/octet-stream';
            if (mimeType.startsWith('image/')) {
              // 如果提供了图片处理器，使用它保存图片并获取 URL
              if (context.imageHandler) {
                const imageUrl = context.imageHandler(part.inlineData.data, mimeType);
                if (emitter.sendImage) {
                  emitter.sendImage(imageUrl, mimeType, part.inlineData.data);
                } else {
                  emitter.sendText?.(`![image](${imageUrl})`);
                }
              } else {
                emitter.sendText(`[Image: ${mimeType}]`);
              }
            } else {
              emitter.sendText(`[Document: ${mimeType}]`);
            }
          }

          // 文件数据：fileData
          if (part.fileData) {
            const mimeType = part.fileData.mimeType || 'application/octet-stream';
            const url = part.fileData.fileUri || '';
            emitter.sendText(`[File: ${mimeType}](${url})`);
          }

          // 函数调用
          if (part.functionCall) {
            hasToolCalls = true;
            const id = part.functionCall.id || generateToolCallId();
            const name = part.functionCall.name;
            const args = JSON.stringify(part.functionCall.args || {});

            // 注册函数调用的 signature
            if (part.thoughtSignature) {
              registerThoughtSignature(id, part.thoughtSignature);
            }

            emitter.sendToolCallStart(id, name);
            emitter.sendToolCallArguments(args);
            emitter.finishToolCall();
          }
        }
      },

      /**
       * 完成流
       */
      finish(finalChunk) {
        const candidate = finalChunk?.candidates?.[0];
        // 传递 hasToolCalls 参数以正确映射 finish_reason
        const finishReason = candidate?.finishReason
          ? mapGeminiStopReason(candidate.finishReason, hasToolCalls).openai
          : 'stop';

        const usage = finalChunk?.usageMetadata;
        emitter.finish(usage, finishReason);
      }
    };
  }

  /**
   * 转换 token 使用统计 (Gemini → OpenAI)
   *
   * 注意：会输出 prompt_tokens_details.cached_tokens
   *
   * @param {object} usageMetadata - Gemini usageMetadata
   * @returns {object} OpenAI usage
   */
  convertUsage(usageMetadata) {
    if (!usageMetadata) {
      return {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0
      };
    }

    const promptTokens = usageMetadata.promptTokenCount || usageMetadata.inputTokenCount || 0;
    const completionTokens = usageMetadata.candidatesTokenCount || usageMetadata.outputTokenCount || 0;
    const cachedTokens = usageMetadata.cachedContentTokenCount || 0;

    const usage = {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
      total_tokens: promptTokens + completionTokens
    };

    // 添加 cached tokens 详情（OpenAI 格式）
    if (cachedTokens > 0) {
      usage.prompt_tokens_details = {
        cached_tokens: cachedTokens
      };
    }

    return usage;
  }

  /**
   * 转换错误响应 (Gemini → OpenAI)
   *
   * @param {Error|object} error - Gemini 错误
   * @returns {object} OpenAI 错误响应
   */
  convertError(error) {
    const message = error?.message || error?.error?.message || 'Unknown error';
    const code = error?.code || error?.error?.code || 'internal_error';

    return {
      error: {
        message,
        type: 'api_error',
        code
      }
    };
  }

  /**
   * 构建空响应 (Gemini → OpenAI)
   *
   * @param {string} requestId - 请求 ID
   * @param {string} model - 模型名称
   * @returns {object} OpenAI Chat Completions 空响应
   */
  buildEmptyResponse(requestId, model) {
    return {
      id: `chatcmpl-${requestId}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model,
      choices: [{
        index: 0,
        message: { role: 'assistant', content: '' },
        finish_reason: 'stop'
      }],
      usage: {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0
      }
    };
  }

  /**
   * 转换 Gemini parts 到 OpenAI 格式 (Gemini → OpenAI)
   *
   * 注意：多模态内容尽可能保留结构（非标准扩展），否则降级为文本
   *
   * @param {Array} parts - Gemini parts 数组
   * @returns {{ content: string|Array, toolCalls: Array, reasoningContent: string|null }} 提取后的内容结构
   */
  convertGeminiToOpenAI(parts) {
    if (!Array.isArray(parts)) {
      return { content: '', toolCalls: [], reasoningContent: null, reasoningSignature: null };
    }

    const mixedParts = [];
    const toolCalls = [];
    let reasoningContent = null;
    let reasoningSignature = null;
    let hasMultimodal = false;

    for (const part of parts) {
      if (!part) continue;

      // 思考内容 → reasoning_content 字段
      if (part.thought === true && part.text) {
        reasoningContent = reasoningContent ? reasoningContent + part.text : part.text;
        // 透传签名 (Scheme B)
        if (part.thoughtSignature) {
          reasoningSignature = part.thoughtSignature;
        }
      }
      // 普通文本
      else if (part.text !== undefined) {
        mixedParts.push({ type: 'text', text: part.text });
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
        mixedParts.push({
          type: 'image_url',
          image_url: {
            url: `data:${part.inlineData.mimeType};base64,${part.inlineData.data}`,
            detail: 'auto'
          }
        });
      }
      // 内联数据（非图片）- 转为 file 扩展类型
      else if (part.inlineData) {
        hasMultimodal = true;
        mixedParts.push({
          type: 'file',
          file: {
            filename: 'document',
            file_data: `data:${part.inlineData.mimeType};base64,${part.inlineData.data}`
          }
        });
      }
      // 文件数据
      else if (part.fileData) {
        hasMultimodal = true;
        const filename = part.fileData.fileUri?.split('/').pop() || 'file';
        mixedParts.push({
          type: 'file',
          file: {
            filename: filename,
            file_data: part.fileData.fileUri
          }
        });
      }
    }

    // 构建 content
    let content;
    if (hasMultimodal) {
      // 存在多模态内容：返回数组（非标准扩展，Chain 3 需要）
      content = mixedParts;
    } else {
      // 纯文本：返回字符串（标准 OpenAI）
      content = mixedParts
        .filter(p => p.type === 'text')
        .map(p => p.text)
        .join('');
    }

    return { content, toolCalls, reasoningContent, reasoningSignature };
  }
}

export default GeminiToOpenAIResponseConverter;
