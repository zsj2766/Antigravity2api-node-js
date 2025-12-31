/**
 * Claude → OpenAI 响应转换器
 *
 * 输入: Claude Messages API 响应格式
 * 输出: OpenAI Chat Completions API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import { OpenAIProtocolEmitter } from '../../utils/converters/common/OpenAIProtocolEmitter.js';
import { generateToolCallId, generateRequestId } from '../../utils/idGenerator.js';
import { mapClaudeStopToOpenAI } from '../../utils/converters/stopReasonMapper.js';
import { safeJsonStringify } from '../../utils/utils.js';

export class ClaudeToOpenAIResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || response.model || 'claude';

    const content = response.content || [];
    const { text, toolCalls } = this.extractContent(content);

    const finishReason = mapClaudeStopToOpenAI(response.stop_reason);
    const usage = this.convertUsage(response.usage);

    const message = {
      role: 'assistant',
      content: text || null
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
      model: context.model || 'claude',
      inputTokens: context.inputTokens || 0
    });

    // 状态：用于累积工具调用参数
    const state = {
      currentToolId: null,
      currentToolName: null,
      toolArgsBuffer: ''
    };

    return {
      emitter,
      state,

      /**
       * 处理 Claude SSE 事件
       */
      process(event) {
        const eventType = event.type;

        switch (eventType) {
          case 'content_block_start':
            const block = event.content_block;
            if (block?.type === 'tool_use') {
              state.currentToolId = block.id || generateToolCallId();
              state.currentToolName = block.name;
              state.toolArgsBuffer = '';
              emitter.sendToolCallStart(state.currentToolId, state.currentToolName);
            }
            break;

          case 'content_block_delta':
            const delta = event.delta;
            if (delta?.type === 'text_delta') {
              emitter.sendText(delta.text);
            } else if (delta?.type === 'thinking_delta') {
              // OpenAI 不支持 thinking，忽略
            } else if (delta?.type === 'input_json_delta') {
              state.toolArgsBuffer += delta.partial_json || '';
              emitter.sendToolCallArguments(delta.partial_json || '');
            }
            break;

          case 'content_block_stop':
            if (state.currentToolId) {
              emitter.finishToolCall();
              state.currentToolId = null;
              state.currentToolName = null;
              state.toolArgsBuffer = '';
            }
            break;

          case 'message_delta':
            // 处理最终状态，但不在这里结束流
            break;
        }
      },

      /**
       * 完成流
       */
      finish(finalEvent) {
        let finishReason = 'stop';
        let usage = null;

        if (finalEvent?.type === 'message_delta') {
          const stopReason = finalEvent.delta?.stop_reason;
          if (stopReason) {
            finishReason = mapClaudeStopToOpenAI(stopReason);
          }
          usage = finalEvent.usage;
        }

        emitter.finish(usage, finishReason);
      }
    };
  }

  /**
   * 转换 token 使用统计
   */
  convertUsage(usage) {
    if (!usage) {
      return {
        prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0
      };
    }

    const promptTokens = usage.input_tokens || 0;
    const completionTokens = usage.output_tokens || 0;

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
    const type = error?.error?.type || 'api_error';

    return {
      error: {
        message,
        type,
        code: null
      }
    };
  }

  /**
   * 从内容块提取文本和工具调用
   */
  extractContent(content) {
    const textParts = [];
    const toolCalls = [];

    for (const block of content) {
      if (!block) continue;

      if (block.type === 'text') {
        textParts.push(block.text || '');
      } else if (block.type === 'tool_use') {
        toolCalls.push({
          id: block.id || generateToolCallId(),
          type: 'function',
          function: {
            name: block.name || 'unknown',
            arguments: safeJsonStringify(block.input) || '{}'
          }
        });
      }
      // thinking 和 redacted_thinking 忽略
    }

    const text = textParts.join('');
    return { text, toolCalls };
  }
}

export default ClaudeToOpenAIResponseConverter;
