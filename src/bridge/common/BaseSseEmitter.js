/**
 * Bridge 内部 SSE Emitter 基类
 */

import { generateRequestId, estimateTokens } from './idUtils.js';

export class BaseSseEmitter {
  constructor(res, options = {}) {
    this.res = res;
    this.requestId = options.requestId || generateRequestId();
    this.model = options.model || 'proxy';
    this.inputTokens = options.inputTokens || 0;

    this.hasStarted = false;
    this.finished = false;
    this.totalOutputTokens = 0;
  }

  trackTokens(text) {
    if (text) {
      this.totalOutputTokens += estimateTokens(text);
    }
  }

  buildUsage(usage) {
    const inputTokens =
      usage?.input_tokens ??
      usage?.prompt_tokens ??
      usage?.promptTokenCount ??
      usage?.inputTokenCount ??
      this.inputTokens;

    const outputTokens =
      usage?.output_tokens ??
      usage?.completion_tokens ??
      usage?.candidatesTokenCount ??
      usage?.outputTokenCount ??
      this.totalOutputTokens;

    return {
      input_tokens: inputTokens,
      output_tokens: outputTokens
    };
  }

  end() {
    if (this.finished) return;
    this.finished = true;
    this.res.end();
  }

  start() {
    throw new Error('start() must be implemented by subclass');
  }

  sendText(text) {
    throw new Error('sendText() must be implemented by subclass');
  }

  sendThinking(thinking) {
    throw new Error('sendThinking() must be implemented by subclass');
  }

  sendToolCalls(toolCalls) {
    throw new Error('sendToolCalls() must be implemented by subclass');
  }

  finish(usage, stopReason) {
    throw new Error('finish() must be implemented by subclass');
  }
}

export default BaseSseEmitter;
