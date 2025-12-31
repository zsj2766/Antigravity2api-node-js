/**
 * Gemini → Claude 响应转换器
 *
 * 输入: Gemini API 响应格式
 * 输出: Claude Messages API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import { ClaudeProtocolEmitter } from '../../utils/converters/common/ClaudeProtocolEmitter.js';
import { generateToolUseId, generateRequestId } from '../../utils/idGenerator.js';
import { mapGeminiStopReason } from '../../utils/converters/stopReasonMapper.js';

export class GeminiToClaudeResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || 'gemini-pro';

    const candidate = response.candidates?.[0];
    if (!candidate) {
      return this.buildEmptyResponse(requestId, model);
    }

    const parts = candidate.content?.parts || [];
    const content = this.extractContent(parts);

    const stopReason = this.mapStopReason(candidate.finishReason, content);
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
   * 创建流式响应处理器
   */
  createStreamProcessor(res, context = {}) {
    const emitter = new ClaudeProtocolEmitter(res, {
      requestId: context.requestId || generateRequestId(),
      model: context.model || 'gemini-pro',
      inputTokens: context.inputTokens || 0
    });

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
          if (part.thought === true && part.text) {
            emitter.sendThinking(part.text);

            // 如果有签名
            if (part.thoughtSignature) {
              emitter.sendSignature(part.thoughtSignature);
            }
            continue;
          }

          // 普通文本
          if (part.text !== undefined) {
            emitter.sendText(part.text);
          }

          // 函数调用
          if (part.functionCall) {
            emitter.sendToolCalls([{
              id: part.functionCall.id || generateToolUseId(),
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
      finish(finalChunk) {
        const candidate = finalChunk?.candidates?.[0];
        let stopReason = 'end_turn';

        if (candidate?.finishReason) {
          stopReason = mapGeminiStopReason(candidate.finishReason);
          // Gemini 的 tool_calls -> Claude 的 tool_use
          if (stopReason === 'tool_calls') {
            stopReason = 'tool_use';
          }
        }

        const usage = finalChunk?.usageMetadata;
        emitter.finish(usage, stopReason);
      }
    };
  }

  /**
   * 转换 token 使用统计
   */
  convertUsage(usageMetadata) {
    if (!usageMetadata) {
      return {
        input_tokens: 0,
        output_tokens: 0
      };
    }

    return {
      input_tokens: usageMetadata.promptTokenCount || 0,
      output_tokens: usageMetadata.candidatesTokenCount || 0
    };
  }

  /**
   * 转换错误响应
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
   * 从 parts 提取内容块
   */
  extractContent(parts) {
    const content = [];

    for (const part of parts) {
      if (!part) continue;

      // 思考内容
      if (part.thought === true && part.text) {
        content.push({
          type: 'thinking',
          thinking: part.text
        });
        continue;
      }

      // 普通文本
      if (part.text !== undefined) {
        content.push({
          type: 'text',
          text: part.text
        });
      }

      // 函数调用
      if (part.functionCall) {
        content.push({
          type: 'tool_use',
          id: part.functionCall.id || generateToolUseId(),
          name: part.functionCall.name,
          input: part.functionCall.args || {}
        });
      }
    }

    return content;
  }

  /**
   * 映射停止原因
   */
  mapStopReason(geminiReason, content) {
    // 如果有 tool_use，返回 tool_use
    const hasToolUse = content.some(c => c.type === 'tool_use');
    if (hasToolUse) {
      return 'tool_use';
    }

    const reasonMap = {
      'STOP': 'end_turn',
      'MAX_TOKENS': 'max_tokens',
      'SAFETY': 'end_turn',
      'RECITATION': 'end_turn',
      'OTHER': 'end_turn'
    };

    return reasonMap[geminiReason] || 'end_turn';
  }

  /**
   * 构建空响应
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
}

export default GeminiToClaudeResponseConverter;
