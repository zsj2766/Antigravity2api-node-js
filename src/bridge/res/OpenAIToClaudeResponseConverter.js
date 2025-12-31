/**
 * OpenAI → Claude 响应转换器
 *
 * 输入: OpenAI Chat Completions API 响应格式
 * 输出: Claude Messages API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import { ClaudeProtocolEmitter } from '../../utils/converters/common/ClaudeProtocolEmitter.js';
import { generateToolUseId, generateRequestId } from '../../utils/idGenerator.js';
import { mapOpenAIFinishToClaude } from '../../utils/converters/stopReasonMapper.js';
import { safeJsonParse } from '../../utils/utils.js';

export class OpenAIToClaudeResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || response.model || 'gpt-4';

    const choice = response.choices?.[0];
    if (!choice) {
      return this.buildEmptyResponse(requestId, model);
    }

    const message = choice.message || {};
    const content = this.extractContent(message);
    const stopReason = mapOpenAIFinishToClaude(choice.finish_reason);
    const usage = this.convertUsage(response.usage);

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
      model: context.model || 'gpt-4',
      inputTokens: context.inputTokens || 0
    });

    // 状态：用于累积工具调用
    const state = {
      toolCalls: {},  // id -> { name, arguments }
      currentToolIndex: null
    };

    return {
      emitter,
      state,

      /**
       * 处理 OpenAI SSE chunk
       */
      process(chunk) {
        const choice = chunk.choices?.[0];
        if (!choice) return;

        const delta = choice.delta || {};

        // 文本内容
        if (delta.content) {
          emitter.sendText(delta.content);
        }

        // 工具调用
        if (delta.tool_calls) {
          for (const tc of delta.tool_calls) {
            const index = tc.index;

            // 新工具调用开始
            if (tc.id) {
              state.toolCalls[index] = {
                id: tc.id,
                name: tc.function?.name || '',
                arguments: ''
              };
              state.currentToolIndex = index;
            }

            // 累积参数
            if (tc.function?.arguments && state.toolCalls[index]) {
              state.toolCalls[index].arguments += tc.function.arguments;
            }
          }
        }
      },

      /**
       * 完成流
       */
      finish(finalChunk) {
        // 发送累积的工具调用
        const toolCallsList = Object.values(state.toolCalls);
        if (toolCallsList.length > 0) {
          const claudeToolCalls = toolCallsList.map(tc => ({
            id: tc.id || generateToolUseId(),
            function: {
              name: tc.name,
              arguments: safeJsonParse(tc.arguments, {})
            }
          }));
          emitter.sendToolCalls(claudeToolCalls);
        }

        // 确定停止原因
        let stopReason = 'end_turn';
        if (finalChunk?.choices?.[0]?.finish_reason) {
          stopReason = mapOpenAIFinishToClaude(finalChunk.choices[0].finish_reason);
        } else if (toolCallsList.length > 0) {
          stopReason = 'tool_use';
        }

        const usage = finalChunk?.usage;
        emitter.finish(usage, stopReason);
      }
    };
  }

  /**
   * 转换 token 使用统计
   */
  convertUsage(usage) {
    if (!usage) {
      return {
        input_tokens: 0,
        output_tokens: 0
      };
    }

    return {
      input_tokens: usage.prompt_tokens || 0,
      output_tokens: usage.completion_tokens || 0
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
   * 从 OpenAI 消息提取 Claude 内容块
   */
  extractContent(message) {
    const content = [];

    // 文本内容
    if (message.content) {
      content.push({
        type: 'text',
        text: message.content
      });
    }

    // 工具调用
    if (message.tool_calls && message.tool_calls.length > 0) {
      for (const tc of message.tool_calls) {
        const args = safeJsonParse(tc.function?.arguments, {});
        content.push({
          type: 'tool_use',
          id: tc.id || generateToolUseId(),
          name: tc.function?.name || 'unknown',
          input: args
        });
      }
    }

    return content;
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

export default OpenAIToClaudeResponseConverter;
