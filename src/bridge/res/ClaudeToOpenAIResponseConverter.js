/**
 * Claude → OpenAI 响应转换器
 *
 * 输入: Claude Messages API 响应格式
 * 输出: OpenAI Chat Completions API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import {
  OpenAIProtocolEmitter,
  generateToolCallId,
  generateRequestId,
  mapClaudeStopToOpenAI
} from '../common/index.js';
import { safeJsonStringify } from '../../utils/utils.js';

export class ClaudeToOpenAIResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换 (Claude → OpenAI)
   *
   * 注意：会映射 stop_reason 并输出 reasoning_content/tool_calls
   *
   * @param {object} response - Claude 响应
   * @param {object} context - 上下文
   * @returns {object} OpenAI Chat Completions 响应
   */
  convert(response, context = {}) {
    const requestId = context.requestId || generateRequestId();
    const model = context.model || response.model || 'claude';

    const content = response.content || [];
    const { text, toolCalls, reasoningContent, reasoningSignature } = this.extractContent(content);

    const finishReason = mapClaudeStopToOpenAI(response.stop_reason);
    const usage = this.convertUsage(response.usage);

    const message = {
      role: 'assistant',
      content: text || null
    };

    // 添加 reasoning_content (OpenAI o1/o3 格式)
    if (reasoningContent) {
      message.reasoning_content = reasoningContent;
    }
    // 添加 reasoning_signature (透传签名)
    if (reasoningSignature) {
      message.reasoning_signature = reasoningSignature;
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
   * 转换响应内容 (Claude → OpenAI)
   *
   * 注意：内部调用 extractContent，返回 { text, toolCalls, reasoningContent }
   *
   * @param {Array} content - Claude 内容块数组
   * @returns {object} 提取后的内容结构
   */
  convertContent(content) {
    return this.extractContent(content || []);
  }

  /**
   * 创建流式响应处理器 (Claude → OpenAI)
   *
   * 注意：返回包含 process(chunk) 与 finish(finalEvent) 的处理器
   *
   * @param {object} res - Express response 对象
   * @param {object} context - 上下文（requestId, model 等）
   * @returns {object} 流处理器
   */
  createStreamProcessor(res, context = {}) {
    const emitter = new OpenAIProtocolEmitter(res, {
      requestId: context.requestId || generateRequestId(),
      model: context.model || 'claude',
      inputTokens: context.inputTokens || 0
    });

    // 状态：用于追踪工具调用和 token 统计
    const state = {
      currentToolId: null,
      inputTokens: 0  // 从 message_start 捕获
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
          case 'message_start':
            // 捕获 input_tokens（仅在 message_start 中可用）
            if (event.message?.usage?.input_tokens) {
              state.inputTokens = event.message.usage.input_tokens;
              emitter.inputTokens = state.inputTokens;
            }
            break;

          case 'content_block_start':
            const block = event.content_block;
            if (block?.type === 'tool_use') {
              const toolId = block.id || generateToolCallId();
              state.currentToolId = toolId;
              emitter.sendToolCallStart(toolId, block.name);
            }
            break;

          case 'content_block_delta':
            const delta = event.delta;
            if (delta?.type === 'text_delta') {
              emitter.sendText(delta.text);
            } else if (delta?.type === 'thinking_delta') {
              // Claude thinking → OpenAI reasoning_content
              emitter.sendThinking(delta.thinking);
            } else if (delta?.type === 'signature_delta') {
              // Claude thinking signature → OpenAI reasoning_signature
              emitter.sendThinking('', delta.signature);
            } else if (delta?.type === 'input_json_delta') {
              emitter.sendToolCallArguments(delta.partial_json || '');
            }
            break;

          case 'content_block_stop':
            if (state.currentToolId) {
              emitter.finishToolCall();
              state.currentToolId = null;
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
          // message_delta 只包含 output_tokens，需要合并 input_tokens
          if (finalEvent.usage) {
            usage = {
              input_tokens: state.inputTokens,
              output_tokens: finalEvent.usage.output_tokens || 0
            };
          }
        }

        emitter.finish(usage, finishReason);
      }
    };
  }

  /**
   * 转换 token 使用统计 (Claude → OpenAI)
   *
   * @param {object} usage - Claude usage
   * @returns {object} OpenAI usage
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
   * 转换错误响应 (Claude → OpenAI)
   *
   * @param {Error|object} error - Claude 错误
   * @returns {object} OpenAI 错误响应
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
   * 从内容块提取文本、工具调用和思考内容 (Claude → OpenAI)
   *
   * 注意：thinking 会映射为 reasoning_content
   *
   * @param {Array} content - Claude 内容块数组
   * @returns {{ text: string, toolCalls: Array, reasoningContent: string|null }} 提取后的内容结构
   */
  extractContent(content) {
    const textParts = [];
    const toolCalls = [];
    const thinkingParts = [];
    let reasoningSignature = null;

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
      } else if (block.type === 'thinking') {
        // Claude thinking → OpenAI reasoning_content
        thinkingParts.push(block.thinking || '');
        // 保留最后一个 signature
        if (block.signature) {
          reasoningSignature = block.signature;
        }
      } else if (block.type === 'redacted_thinking') {
        // redacted_thinking 不包含可读内容，但可透传签名
        const signature = block.signature || block.data;
        if (signature) {
          reasoningSignature = signature;
        }
      }
    }

    const text = textParts.join('');
    const reasoningContent = thinkingParts.length > 0 ? thinkingParts.join('') : null;
    return { text, toolCalls, reasoningContent, reasoningSignature };
  }

  /**
   * 构建空响应 (Claude → OpenAI)
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
}

export default ClaudeToOpenAIResponseConverter;
