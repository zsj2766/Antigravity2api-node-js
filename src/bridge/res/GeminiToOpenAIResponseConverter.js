/**
 * Gemini → OpenAI 响应转换器
 *
 * 输入: Gemini API 响应格式
 * 输出: OpenAI Chat Completions API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import { OpenAIProtocolEmitter } from '../../utils/converters/common/OpenAIProtocolEmitter.js';
import { generateToolCallId, generateRequestId } from '../../utils/idGenerator.js';
import { mapGeminiStopReason } from '../../utils/converters/stopReasonMapper.js';

export class GeminiToOpenAIResponseConverter extends IResponseConverter {
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
    const { content, toolCalls } = this.extractContent(parts);

    const finishReason = mapGeminiStopReason(candidate.finishReason);
    const usage = this.convertUsage(response.usageMetadata);

    const message = {
      role: 'assistant',
      content: content || null
    };

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
   * 创建流式响应处理器
   */
  createStreamProcessor(res, context = {}) {
    const emitter = new OpenAIProtocolEmitter(res, {
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

          // 文本内容
          if (part.text !== undefined) {
            // 跳过 thinking（Gemini 的 thought: true）
            if (part.thought === true) {
              // OpenAI 不支持 thinking，忽略
              continue;
            }
            emitter.sendText(part.text);
          }

          // 函数调用
          if (part.functionCall) {
            const id = part.functionCall.id || generateToolCallId();
            const name = part.functionCall.name;
            const args = JSON.stringify(part.functionCall.args || {});

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
        const finishReason = candidate?.finishReason
          ? mapGeminiStopReason(candidate.finishReason)
          : 'stop';

        const usage = finalChunk?.usageMetadata;
        emitter.finish(usage, finishReason);
      }
    };
  }

  /**
   * 转换 token 使用统计
   */
  convertUsage(usageMetadata) {
    if (!usageMetadata) {
      return {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0
      };
    }

    const promptTokens = usageMetadata.promptTokenCount || 0;
    const completionTokens = usageMetadata.candidatesTokenCount || 0;

    return {
      prompt_tokens: promptTokens,
      completion_tokens: completionTokens,
      total_tokens: promptTokens + completionTokens
    };
  }

  /**
   * 转换错误响应
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
   * 从 parts 提取内容
   */
  extractContent(parts) {
    const textParts = [];
    const toolCalls = [];

    for (const part of parts) {
      if (!part) continue;

      // 跳过 thinking
      if (part.thought === true) continue;

      if (part.text !== undefined) {
        textParts.push(part.text);
      }

      if (part.functionCall) {
        toolCalls.push({
          id: part.functionCall.id || generateToolCallId(),
          type: 'function',
          function: {
            name: part.functionCall.name,
            arguments: JSON.stringify(part.functionCall.args || {})
          }
        });
      }
    }

    const content = textParts.join('');
    return { content, toolCalls };
  }

  /**
   * 构建空响应
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
}

export default GeminiToOpenAIResponseConverter;
