/**
 * Bridge 内部 OpenAI 协议 SSE Emitter
 */

import { BaseSseEmitter } from './BaseSseEmitter.js';
import { generateToolCallId } from './idUtils.js';

export class OpenAIProtocolEmitter extends BaseSseEmitter {
  constructor(res, options = {}) {
    super(res, options);
    this.toolCallIndex = 0;
    this.includeUsage = options.includeUsage === true;
  }

  writeData(data) {
    if (this.res?.locals) {
      this.res.locals.streamBodySent = true;
    }
    // 收集事件用于日志记录
    this.collectEvent('data', data);
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  buildChunkBase() {
    return {
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model
    };
  }

  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;

    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: { role: 'assistant', content: '' },
        finish_reason: null
      }]
    });
  }

  sendText(text) {
    if (!text || this.finished) return;
    if (!this.hasStarted) this.start();

    this.trackTokens(text);

    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: { content: text },
        finish_reason: null
      }]
    });
  }

  sendThinking(thinking, signature) {
    if (this.finished) return;
    if (!thinking && !signature) return;
    if (!this.hasStarted) this.start();

    if (thinking) {
      this.trackTokens(thinking);
    }

    const delta = {};
    if (thinking) {
      delta.reasoning_content = thinking;
    }
    if (signature) {
      delta.reasoning_signature = signature;
    }

    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta,
        finish_reason: null
      }]
    });
  }

  sendToolCallStart(id, name) {
    if (this.finished) return;
    if (!this.hasStarted) this.start();

    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: {
          tool_calls: [{
            index: this.toolCallIndex,
            id: id || generateToolCallId(),
            type: 'function',
            function: { name, arguments: '' }
          }]
        },
        finish_reason: null
      }]
    });
  }

  sendToolCallArguments(args) {
    if (this.finished) return;

    const argsStr = typeof args === 'string' ? args : JSON.stringify(args);
    this.trackTokens(argsStr);

    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: {
          tool_calls: [{
            index: this.toolCallIndex,
            function: { arguments: argsStr }
          }]
        },
        finish_reason: null
      }]
    });
  }

  finishToolCall() {
    this.toolCallIndex++;
  }

  sendToolCalls(toolCalls) {
    if (!toolCalls || toolCalls.length === 0 || this.finished) return;
    if (!this.hasStarted) this.start();

    for (const call of toolCalls) {
      const id = call.id || generateToolCallId();
      const name = call?.function?.name || 'tool';
      const args = call?.function?.arguments || '{}';
      const argsStr = typeof args === 'string' ? args : JSON.stringify(args);

      this.trackTokens(argsStr);
      this.sendToolCallStart(id, name);
      this.sendToolCallArguments(argsStr);
      this.finishToolCall();
    }
  }

  finish(usage, finishReason = 'stop') {
    if (this.finished) return;
    this.finished = true;

    const finalUsage = this.buildUsage(usage);
    const cachedTokens = usage?.prompt_tokens_details?.cached_tokens ?? usage?.cachedContentTokenCount ?? 0;
    const reasoningTokens = usage?.completion_tokens_details?.reasoning_tokens ?? usage?.thoughtsTokenCount ?? 0;

    const payload = {
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: {},
        finish_reason: finishReason
      }]
    };

    if (this.includeUsage) {
      payload.usage = {
        prompt_tokens: finalUsage.input_tokens,
        completion_tokens: finalUsage.output_tokens,
        total_tokens: finalUsage.input_tokens + finalUsage.output_tokens
      };

      if (cachedTokens > 0) {
        payload.usage.prompt_tokens_details = {
          cached_tokens: cachedTokens
        };
      }

      if (reasoningTokens > 0) {
        payload.usage.completion_tokens_details = {
          reasoning_tokens: reasoningTokens
        };
      }
    }

    this.writeData(payload);

    this.res.write('data: [DONE]\n\n');
    this.res.end();
  }
}

export default OpenAIProtocolEmitter;
