/**
 * Gemini → Claude 响应转换器
 *
 * 输入: Gemini API 响应格式
 * 输出: Claude Messages API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import {
  ClaudeProtocolEmitter,
  generateToolUseId,
  generateRequestId,
  mapGeminiStopReason
} from '../common/index.js';
import { registerTextThoughtSignature, registerThoughtSignature } from '../../utils/utils.js';
import log from '../../utils/logger.js';

export class GeminiToClaudeResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换 (Gemini → Claude)
   *
   * 注意：会注册 thoughtSignature 并依据 tool_use 映射 stop_reason
   *
   * @param {object} response - Gemini 响应
   * @param {object} context - 上下文（requestId, model, imageHandler 等）
   * @returns {object} Claude Messages 响应
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || 'gemini-pro';

    const candidate = response.candidates?.[0];
    if (!candidate) {
      return this.buildEmptyResponse(requestId, model);
    }

    const parts = candidate.content?.parts || [];

    // 使用内联方法处理多模态内容（内部已处理 thoughtSignature 注册）
    const content = this.convertGeminiToClaude(parts, context.imageHandler);

    // 使用 mapGeminiStopReason 并传递 hasToolUse 参数
    const hasToolUse = content.some(c => c.type === 'tool_use');
    const stopReason = mapGeminiStopReason(candidate.finishReason, hasToolUse).claude;
    const usage = this.convertUsage(response.usageMetadata);

    return {
      id: `msg_${requestId}`,
      type: 'message',
      role: 'assistant',
      model,
      content,
      stop_reason: stopReason,
      stop_sequence: null,
      usage
    };
  }

  /**
   * 转换响应内容 (Gemini → Claude)
   *
   * @param {Array} parts - Gemini parts 数组
   * @param {Function} [imageHandler] - 可选的图片处理器
   * @returns {Array} Claude content 块数组
   */
  convertContent(parts, imageHandler = null) {
    return this.convertGeminiToClaude(parts, imageHandler);
  }

  /**
   * 创建流式响应处理器 (Gemini → Claude)
   *
   * @param {object} resOrEmitter - Express response 对象或自定义 Emitter 实例
   * @param {object} context - 上下文（requestId, model, imageHandler 等）
   * @returns {object} 流处理器
   */
  createStreamProcessor(resOrEmitter, context = {}) {
    // 支持注入自定义 Emitter（鸭子类型检查）
    const emitter = (resOrEmitter && typeof resOrEmitter.sendText === 'function' && typeof resOrEmitter.finish === 'function')
      ? resOrEmitter
      : new ClaudeProtocolEmitter(resOrEmitter, {
          requestId: context.requestId || generateRequestId(),
          model: context.model || 'gemini-pro',
          inputTokens: context.inputTokens || 0
        });

    // 追踪是否有工具调用
    let hasToolUse = false;

    return {
      emitter,

      /**
       * 处理 Gemini 流式 chunk
       */
      process(chunk) {
        const candidate = chunk.candidates?.[0];
        if (!candidate) return;

        const parts = candidate.content?.parts || [];

        for (const part of parts) {
          if (!part) continue;

          // 思考内容
          if (part.thought === true) {
            if (part.text) {
              // 注册 signature 以便后续请求回传
              if (part.thoughtSignature) {
                registerTextThoughtSignature(part.text, part.thoughtSignature);
              }
              emitter.sendThinking(part.text);

              // 发送签名到 Claude 客户端
              if (part.thoughtSignature) {
                emitter.sendSignature(part.thoughtSignature);
              }
            } else if (part.thoughtSignature) {
              // 单独的 signature chunk（没有 text）
              emitter.sendSignature(part.thoughtSignature);
            }
            continue;
          }

          // 普通文本
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
              if (emitter.sendImage) {
                emitter.sendImage(part.inlineData.data, mimeType);
              } else if (context.imageHandler) {
                // 如果提供了图片处理器，使用它保存图片并获取 URL
                const imageUrl = context.imageHandler(part.inlineData.data, mimeType);
                emitter.sendText(`![image](${imageUrl})`);
              } else {
                emitter.sendText(`[Image: ${mimeType}]`);
              }
            } else {
              if (emitter.sendDocument) {
                emitter.sendDocument(part.inlineData.data, mimeType);
              } else {
                emitter.sendText(`[Document: ${mimeType}]`);
              }
            }
          }

          // 文件数据：fileData
          // TODO: Gemini 返回的是 fileUri (URL)，Claude 需要 base64 数据
          // 完整实现需要 HTTP 下载 fileUri 内容并转换为 base64，暂不支持
          if (part.fileData) {
            const mimeType = part.fileData.mimeType || 'application/octet-stream';
            const url = part.fileData.fileUri || '';
            emitter.sendText(`[File: ${mimeType}](${url})`);
          }

          // 函数调用
          if (part.functionCall) {
            hasToolUse = true;
            const id = part.functionCall.id || generateToolUseId();

            // 注册函数调用的 signature
            if (part.thoughtSignature) {
              registerThoughtSignature(id, part.thoughtSignature);
            }

            emitter.sendToolCalls([{
              id,
              function: {
                name: part.functionCall.name,
                arguments: part.functionCall.args || {}
              }
            }]);
          }
        }
      },

      /**
       * 完成流
       */
      finish(finalChunk, overrideStopReason) {
        const candidate = finalChunk?.candidates?.[0];
        let stopReason = overrideStopReason || 'end_turn';

        if (!overrideStopReason && candidate?.finishReason) {
          // 传递 hasToolUse 参数以正确映射 stop_reason
          stopReason = mapGeminiStopReason(candidate.finishReason, hasToolUse).claude;
        }

        const usage = finalChunk?.usageMetadata;
        emitter.finish(usage, stopReason);
      }
    };
  }

  /**
   * 转换 token 使用统计 (Gemini → Claude)
   *
   * 注意：会输出缓存相关 token 字段
   *
   * @param {object} usageMetadata - Gemini usageMetadata
   * @returns {object} Claude usage
   */
  convertUsage(usageMetadata) {
    if (!usageMetadata) {
      return {
        input_tokens: 0,
        output_tokens: 0
      };
    }

    const inputTokens = usageMetadata.promptTokenCount || usageMetadata.inputTokenCount || 0;
    const outputTokens = usageMetadata.candidatesTokenCount || usageMetadata.outputTokenCount || 0;
    const cachedTokens = usageMetadata.cachedContentTokenCount || 0;

    return {
      input_tokens: inputTokens,
      output_tokens: outputTokens,
      cache_read_input_tokens: cachedTokens,
      cache_creation_input_tokens: 0
    };
  }

  /**
   * 转换错误响应 (Gemini → Claude)
   *
   * @param {Error|object} error - Gemini 错误
   * @returns {object} Claude 错误响应
   */
  convertError(error) {
    const message = error?.message || error?.error?.message || 'Unknown error';

    return {
      type: 'error',
      error: {
        type: 'api_error',
        message
      }
    };
  }

  /**
   * 构建空响应 (Gemini → Claude)
   *
   * @param {string} requestId - 请求 ID
   * @param {string} model - 模型名称
   * @returns {object} Claude 空响应
   */
  buildEmptyResponse(requestId, model) {
    return {
      id: `msg_${requestId}`,
      type: 'message',
      role: 'assistant',
      model,
      content: [],
      stop_reason: 'end_turn',
      stop_sequence: null,
      usage: {
        input_tokens: 0,
        output_tokens: 0
      }
    };
  }

  /**
   * 转换 Gemini parts 到 Claude content blocks (Gemini → Claude)
   *
   * @param {Array} parts - Gemini parts 数组
   * @param {Function} [imageHandler] - 可选的图片处理器，用于保存图片并返回 URL
   * @returns {Array} Claude content 块数组
   */
  convertGeminiToClaude(parts, imageHandler = null) {
    if (!Array.isArray(parts)) {
      return [];
    }

    const blocks = [];
    let lastThinkingBlock = null;
    let pendingThinkingSignature = null;
    let pendingThinkingSignatureIndex = null;

    for (const part of parts) {
      if (!part) continue;

      // 思考内容 → Claude thinking 块
      if (part.thought === true) {
        if (part.text !== undefined) {
          if (part.thoughtSignature) {
            registerTextThoughtSignature(part.text, part.thoughtSignature);
          }
          const thinkingBlock = {
            type: 'thinking',
            thinking: part.text ?? ''
          };
          // 透传签名到 Claude 响应
          if (part.thoughtSignature) {
            thinkingBlock.signature = part.thoughtSignature;
          } else if (pendingThinkingSignature) {
            thinkingBlock.signature = pendingThinkingSignature;
            pendingThinkingSignature = null;
            pendingThinkingSignatureIndex = null;
          }
          blocks.push(thinkingBlock);
          lastThinkingBlock = thinkingBlock;
        } else if (part.thoughtSignature) {
          if (lastThinkingBlock && !lastThinkingBlock.signature) {
            lastThinkingBlock.signature = part.thoughtSignature;
          } else {
            pendingThinkingSignature = part.thoughtSignature;
            pendingThinkingSignatureIndex = blocks.length;
          }
        }
        continue;
      }
      // 普通文本
      else if (part.text !== undefined) {
        if (part.thoughtSignature) {
          registerTextThoughtSignature(part.text, part.thoughtSignature);
        }
        blocks.push({
          type: 'text',
          text: part.text
        });
      }
      // 函数调用 → Claude tool_use 块
      else if (part.functionCall) {
        const id = part.functionCall.id || generateToolUseId();
        if (part.thoughtSignature) {
          registerThoughtSignature(id, part.thoughtSignature);
        }
        blocks.push({
          type: 'tool_use',
          id,
          name: part.functionCall.name,
          input: part.functionCall.args || {}
        });
      }
      // 内联数据（图片）
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
      // 内联数据（文档）
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
      // 文件数据 - 降级为文本占位符
      // TODO: Gemini 返回的是 fileUri (URL)，Claude 需要 base64 数据
      // 完整实现需要 HTTP 下载 fileUri 内容并转换为 base64，暂不支持
      else if (part.fileData) {
        const mimeType = part.fileData.mimeType || 'application/octet-stream';
        const url = part.fileData.fileUri || '';
        blocks.push({
          type: 'text',
          text: `[File: ${mimeType}](${url})`
        });
      }
    }

    if (pendingThinkingSignature) {
      const signatureBlock = {
        type: 'thinking',
        thinking: '',
        signature: pendingThinkingSignature
      };
      // 插入到原始位置而非追加到末尾，保持内容顺序
      if (pendingThinkingSignatureIndex === null || pendingThinkingSignatureIndex >= blocks.length) {
        blocks.push(signatureBlock);
      } else {
        blocks.splice(pendingThinkingSignatureIndex, 0, signatureBlock);
      }
    }

    return blocks;
  }
}

export default GeminiToClaudeResponseConverter;
