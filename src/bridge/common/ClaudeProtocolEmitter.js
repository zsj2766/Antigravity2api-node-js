/**
 * Bridge 内部 Claude 协议 SSE Emitter
 */

import { BaseSseEmitter } from './BaseSseEmitter.js';
import { generateToolUseId, safeChunkString } from './idUtils.js';
import log from '../../utils/logger.js';

export class ClaudeProtocolEmitter extends BaseSseEmitter {
  constructor(res, options = {}) {
    super(res, options);

    this.nextIndex = 0;
    this.textBlockIndex = null;
    this.thinkingBlockIndex = null;
    this.currentBlockType = null;
  }

  writeEvent(event, data) {
    if (this.res?.locals) {
      this.res.locals.streamBodySent = true;
    }
    this.res.write(`event: ${event}\n`);
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  buildMessageStartPayload() {
    return {
      type: 'message_start',
      message: {
        id: `msg_${this.requestId}`,
        type: 'message',
        role: 'assistant',
        model: this.model || 'claude-proxy',
        stop_sequence: null,
        usage: {
          input_tokens: this.inputTokens || 0,
          output_tokens: 0
        },
        content: [],
        stop_reason: null
      }
    };
  }

  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    this.writeEvent('message_start', this.buildMessageStartPayload());
  }

  ensureBlock(type) {
    if (!this.hasStarted) this.start();

    if (this.currentBlockType && this.currentBlockType !== type) {
      this.closeCurrentBlock();
    }

    if (type === 'text' && this.textBlockIndex !== null) {
      return this.textBlockIndex;
    }
    if (type === 'thinking' && this.thinkingBlockIndex !== null) {
      return this.thinkingBlockIndex;
    }

    const index = this.nextIndex++;
    this.currentBlockType = type;

    if (type === 'text') {
      this.textBlockIndex = index;
      this.writeEvent('content_block_start', {
        type: 'content_block_start',
        index,
        content_block: { type: 'text', text: '' }
      });
    } else if (type === 'thinking') {
      this.thinkingBlockIndex = index;
      this.writeEvent('content_block_start', {
        type: 'content_block_start',
        index,
        content_block: { type: 'thinking', thinking: '', signature: '' }
      });
    }

    return index;
  }

  closeCurrentBlock() {
    if (this.currentBlockType === 'text') {
      this.closeTextBlock();
    } else if (this.currentBlockType === 'thinking') {
      this.closeThinkingBlock();
    }
    this.currentBlockType = null;
  }

  closeTextBlock() {
    if (this.textBlockIndex === null) return;
    const index = this.textBlockIndex;
    this.textBlockIndex = null;
    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  closeThinkingBlock() {
    if (this.thinkingBlockIndex === null) return;
    const index = this.thinkingBlockIndex;
    this.thinkingBlockIndex = null;
    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  sendText(text) {
    if (!text || this.finished) return;

    const index = this.ensureBlock('text');
    this.trackTokens(text);

    this.writeEvent('content_block_delta', {
      type: 'content_block_delta',
      index,
      delta: { type: 'text_delta', text }
    });
  }

  sendImage(base64Data, mimeType) {
    if (!base64Data || this.finished) return;

    if (!this.hasStarted) this.start();
    this.closeCurrentBlock();

    const index = this.nextIndex++;

    this.writeEvent('content_block_start', {
      type: 'content_block_start',
      index,
      content_block: {
        type: 'image',
        source: {
          type: 'base64',
          media_type: mimeType,
          data: base64Data
        }
      }
    });

    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  sendDocument(base64Data, mimeType) {
    if (!base64Data || this.finished) return;

    if (!this.hasStarted) this.start();
    this.closeCurrentBlock();

    const index = this.nextIndex++;

    this.writeEvent('content_block_start', {
      type: 'content_block_start',
      index,
      content_block: {
        type: 'document',
        source: {
          type: 'base64',
          media_type: mimeType,
          data: base64Data
        }
      }
    });

    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  sendThinking(thinking) {
    if (!thinking || this.finished) return;

    const index = this.ensureBlock('thinking');
    this.trackTokens(thinking);

    this.writeEvent('content_block_delta', {
      type: 'content_block_delta',
      index,
      delta: { type: 'thinking_delta', thinking }
    });
  }

  sendSignature(signature) {
    if (!signature || this.finished) return;

    if (this.thinkingBlockIndex === null) {
      this.ensureBlock('thinking');
    }

    this.writeEvent('content_block_delta', {
      type: 'content_block_delta',
      index: this.thinkingBlockIndex,
      delta: { type: 'signature_delta', signature }
    });
  }

  sendToolCalls(toolCalls) {
    if (!toolCalls || toolCalls.length === 0 || this.finished) return;
    if (!this.hasStarted) this.start();

    this.closeCurrentBlock();

    for (const call of toolCalls) {
      const index = this.nextIndex++;
      const args = call?.function?.arguments ?? '{}';
      const inputJson = typeof args === 'string' ? args : JSON.stringify(args);

      this.trackTokens(inputJson);

      const toolUseBlock = {
        type: 'tool_use',
        id: call.id || generateToolUseId(),
        name: call?.function?.name || 'tool',
        input: {}
      };

      this.writeEvent('content_block_start', {
        type: 'content_block_start',
        index,
        content_block: toolUseBlock
      });

      // 使用安全的字符串分块，确保不切断多字节字符
      const CHUNK_SIZE = 128;
      const chunks = safeChunkString(inputJson, CHUNK_SIZE);

      for (const chunk of chunks) {
        this.writeEvent('content_block_delta', {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: chunk }
        });
      }

      this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
    }
  }

  finish(usage, stopReason = 'end_turn', extraUsage = null) {
    if (this.finished) return;
    this.finished = true;

    // 确保 message_start 已发送（空流场景保护）
    if (!this.hasStarted) {
      this.start();
    }

    this.closeCurrentBlock();

    const finalUsage = {
      ...this.buildUsage(usage),
      cache_creation_input_tokens: usage?.cache_creation_input_tokens ?? 0,
      cache_read_input_tokens: usage?.cache_read_input_tokens ??
        usage?.prompt_tokens_details?.cached_tokens ??
        usage?.cachedContentTokenCount ?? 0,
      ...(extraUsage || {})
    };

    this.writeEvent('message_delta', {
      type: 'message_delta',
      delta: { stop_reason: stopReason, stop_sequence: null },
      usage: finalUsage
    });

    this.writeEvent('message_stop', { type: 'message_stop' });
    this.res.end();
  }
}

export default ClaudeProtocolEmitter;
