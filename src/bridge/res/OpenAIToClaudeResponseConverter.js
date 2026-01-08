/**
 * OpenAI → Claude 响应转换器
 *
 * 输入: OpenAI Chat Completions API 响应格式
 * 输出: Claude Messages API 响应格式
 */

import { IResponseConverter } from '../interfaces/IResponseConverter.js';
import {
  ClaudeProtocolEmitter,
  generateToolUseId,
  generateRequestId,
  mapOpenAIFinishToClaude
} from '../common/index.js';
import { safeJsonParse, getTextThoughtSignature } from '../../utils/utils.js';

export class OpenAIToClaudeResponseConverter extends IResponseConverter {
  /**
   * 非流式响应转换 (OpenAI → Claude)
   *
   * 注意：无候选结果时返回空响应
   *
   * @param {object} response - OpenAI 响应
   * @param {object} context - 上下文
   * @returns {object} Claude Messages 响应
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
   * 转换响应内容 (OpenAI → Claude)
   *
   * 注意：内部调用 extractContent
   *
   * @param {object} message - OpenAI message
   * @returns {Array} Claude content 块数组
   */
  convertContent(message) {
    return this.extractContent(message || {});
  }

  /**
   * 创建流式响应处理器 (OpenAI → Claude)
   *
   * 注意：返回包含 process(chunk) 与 finish(finalChunk) 的处理器
   *
   * @param {object} res - Express response 对象
   * @param {object} context - 上下文（requestId, model 等）
   * @returns {object} 流处理器
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

        // 推理内容（OpenAI o1/o3 系列的 reasoning_content → Claude thinking）
        if (delta.reasoning_content) {
          // 优先使用透传签名 (Scheme B)
          if (delta.reasoning_signature) {
            emitter.sendSignature(delta.reasoning_signature);
          } else {
            // 降级：尝试从缓存恢复签名（非流式场景可能有效）
            const sig = getTextThoughtSignature(delta.reasoning_content);
            if (sig?.signature) {
              emitter.sendSignature(sig.signature);
            }
          }
          emitter.sendThinking(delta.reasoning_content);
        }

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
   * 转换 token 使用统计 (OpenAI → Claude)
   *
   * @param {object} usage - OpenAI usage
   * @returns {object} Claude usage
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
   * 转换错误响应 (OpenAI → Claude)
   *
   * @param {Error|object} error - OpenAI 错误
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
   * 从 OpenAI 消息提取 Claude 内容块 (OpenAI → Claude)
   *
   * 注意：reasoning_content 会映射为 Claude thinking
   *
   * @param {object} message - OpenAI message
   * @returns {Array} Claude content 块数组
   */
  extractContent(message) {
    const content = [];

    // 推理内容（OpenAI o1/o3 系列的 reasoning_content → Claude thinking）
    if (message.reasoning_content) {
      const thinkingBlock = {
        type: 'thinking',
        thinking: message.reasoning_content
      };

      // 优先使用透传签名 (Scheme B)
      if (message.reasoning_signature) {
        thinkingBlock.signature = message.reasoning_signature;
      } else {
        // 降级：尝试从缓存恢复签名
        const sig = getTextThoughtSignature(message.reasoning_content);
        if (sig?.signature) {
          thinkingBlock.signature = sig.signature;
        }
      }
      content.push(thinkingBlock);
    }

    // 文本与多模态内容（处理 Chain 3 中的非标准多模态扩展）
    if (message.content) {
      if (Array.isArray(message.content)) {
        for (const part of message.content) {
          if (!part) continue;

          if (part.type === 'text' && part.text) {
            content.push({ type: 'text', text: part.text });
          } else if (part.type === 'image_url') {
            // OpenAI image_url → Claude image
            const url = part.image_url?.url || '';
            const isBase64 = url.startsWith('data:');

            if (isBase64) {
              const match = url.match(/^data:([^;]+);base64,(.+)$/);
              if (match) {
                content.push({
                  type: 'image',
                  source: {
                    type: 'base64',
                    media_type: match[1],
                    data: match[2]
                  }
                });
              }
            } else {
              // URL 类型
              content.push({
                type: 'image',
                source: {
                  type: 'url',
                  url: url
                }
              });
            }
          } else if (part.type === 'file') {
             // OpenAI file 扩展 → Claude document
             const fileData = part.file?.file_data || '';
             const filename = part.file?.filename || 'file';
             const match = fileData.match(/^data:([^;]+);base64,(.+)$/);
             if (match) {
               content.push({
                 type: 'document',
                 source: {
                   type: 'base64',
                   media_type: match[1],
                   data: match[2]
                 }
               });
             } else {
               content.push({
                 type: 'text',
                 text: `[File: ${filename}]`
               });
             }
          }
        }
      } else {
        content.push({ type: 'text', text: message.content });
      }
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
   * 构建空响应 (OpenAI → Claude)
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
}

export default OpenAIToClaudeResponseConverter;
