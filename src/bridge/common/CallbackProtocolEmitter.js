/**
 * 回调协议 Emitter
 *
 * 将 Emitter 接口转换为回调函数调用
 * 用于 api/client.js 与 Bridge 响应转换器的适配
 *
 * 实现与 OpenAIProtocolEmitter 相同的接口，但不直接写入 HTTP 响应，
 * 而是将事件通过回调函数传递给上层调用者
 */

import { generateToolCallId } from './idUtils.js';

export class CallbackProtocolEmitter {
  /**
   * @param {Function} callback - 回调函数，接收 { type, content, ... } 格式的事件
   * @param {object} state - 共享状态对象，用于返回 usage 和 finishReason
   * @param {object} options - 配置选项
   */
  constructor(callback, state, options = {}) {
    this.callback = callback;
    this.state = state;
    this.options = options;

    this.hasStarted = false;
    this.finished = false;
    this.toolCallIndex = 0;
    this.currentToolCall = null;

    // Token 追踪
    this.inputTokens = options.inputTokens || 0;
    this.outputTokens = 0;
  }

  /**
   * 估算文本 token 数量（简单估算）
   */
  trackTokens(text) {
    if (text) {
      // 简单估算：每 4 个字符约 1 个 token
      this.outputTokens += Math.ceil(text.length / 4);
    }
  }

  /**
   * 开始流（可选，保持接口兼容）
   */
  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
  }

  /**
   * 发送文本内容
   */
  sendText(text) {
    if (!text || this.finished) return;
    if (!this.hasStarted) this.start();

    this.trackTokens(text);
    this.callback({ type: 'text', content: text });
  }

  /**
   * 发送思考内容
   */
  sendThinking(thinking, signature) {
    // 允许空 thinking 但有 signature 的情况（signature 单独到达）
    if ((!thinking && !signature) || this.finished) return;
    if (!this.hasStarted) this.start();

    if (thinking) {
      this.trackTokens(thinking);
    }
    this.callback({
      type: 'thinking',
      content: thinking || '',
      signature: signature || null
    });
  }

  /**
   * 发送签名（单独到达的 signature）
   */
  sendSignature(signature) {
    if (!signature || this.finished) return;
    if (!this.hasStarted) this.start();

    this.callback({
      type: 'thinking',
      content: '',
      signature: signature
    });
  }

  /**
   * 发送图片内容
   */
  sendImage(url, mimeType, data) {
    if (!url || this.finished) return;
    if (!this.hasStarted) this.start();

    this.callback({
      type: 'image',
      url: url,
      mimeType: mimeType || 'image/png',
      data: data || null
    });
  }

  /**
   * 开始工具调用
   */
  sendToolCallStart(id, name) {
    if (this.finished) return;
    if (!this.hasStarted) this.start();

    this.currentToolCall = {
      id: id || generateToolCallId(),
      function: {
        name: name,
        arguments: ''
      }
    };
  }

  /**
   * 发送工具调用参数
   */
  sendToolCallArguments(args) {
    if (this.finished || !this.currentToolCall) return;

    const argsStr = typeof args === 'string' ? args : JSON.stringify(args);
    this.currentToolCall.function.arguments += argsStr;
    this.trackTokens(argsStr);
  }

  /**
   * 完成当前工具调用
   */
  finishToolCall() {
    if (this.currentToolCall) {
      // 发送完整的工具调用事件
      this.callback({
        type: 'tool_call_chunk',
        tool_call: {
          index: this.toolCallIndex,
          id: this.currentToolCall.id,
          function: this.currentToolCall.function
        }
      });

      this.toolCallIndex++;
      this.currentToolCall = null;
    }
  }

  /**
   * 批量发送工具调用（兼容旧接口）
   */
  sendToolCalls(toolCalls) {
    if (!toolCalls || toolCalls.length === 0 || this.finished) return;
    if (!this.hasStarted) this.start();

    for (const call of toolCalls) {
      const id = call.id || generateToolCallId();
      const name = call?.function?.name || 'tool';
      const args = call?.function?.arguments || '{}';

      this.sendToolCallStart(id, name);
      this.sendToolCallArguments(args);
      this.finishToolCall();
    }
  }

  /**
   * 构建 usage 对象
   */
  buildUsage(upstreamUsage) {
    if (upstreamUsage) {
      return {
        input_tokens: upstreamUsage.promptTokenCount || upstreamUsage.inputTokenCount || this.inputTokens,
        output_tokens: upstreamUsage.candidatesTokenCount || upstreamUsage.outputTokenCount || this.outputTokens
      };
    }
    return {
      input_tokens: this.inputTokens,
      output_tokens: this.outputTokens
    };
  }

  /**
   * 完成流
   */
  finish(usage, finishReason = 'stop') {
    if (this.finished) return;
    this.finished = true;

    const finalUsage = this.buildUsage(usage);

    // 更新共享状态
    this.state.usage = {
      prompt_tokens: finalUsage.input_tokens,
      completion_tokens: finalUsage.output_tokens,
      total_tokens: finalUsage.input_tokens + finalUsage.output_tokens
    };
    this.state.finishReason = finishReason;

    // 发送完成事件（可选，上层可以忽略）
    this.callback({
      type: 'finish_reason',
      finishReason: finishReason
    });
  }
}

export default CallbackProtocolEmitter;
