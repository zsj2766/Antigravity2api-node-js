/**
 * 【响应转换】Claude 协议 SSE Emitter
 *
 * 输出格式: Claude Messages API SSE
 *
 * 实现 Claude Messages API 的 SSE 协议格式：
 * - event: message_start / content_block_start / content_block_delta / content_block_stop / message_delta / message_stop
 * - Block 生命周期管理（index 递增）
 *
 * 子类：ClaudeSseEmitter, OpenAIToClaudeSseEmitter
 */

import { BaseSseEmitter } from './BaseSseEmitter.js';
import { generateToolUseId } from '../../idGenerator.js';
import { writeSSE, buildMessageStartPayload } from '../sseUtils.js';

export class ClaudeProtocolEmitter extends BaseSseEmitter {
  constructor(res, options = {}) {
    super(res, options);

    // Block 索引管理
    this.nextIndex = 0;
    this.textBlockIndex = null;
    this.thinkingBlockIndex = null;

    // 当前活动块类型
    this.currentBlockType = null;
  }

  /**
   * 写入 Claude SSE 事件
   * @param {string} event - 事件类型
   * @param {object} data - 数据
   */
  writeEvent(event, data) {
    writeSSE(this.res, event, data);
  }

  /**
   * 启动 SSE 流
   */
  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    this.writeEvent('message_start', buildMessageStartPayload(this.requestId, this.model, this.inputTokens));
  }

  /**
   * 确保指定类型的 Block 已开启
   * 自动关闭其他类型的 Block
   * @param {string} type - 'text' | 'thinking'
   * @returns {number} - Block 索引
   */
  ensureBlock(type) {
    if (!this.hasStarted) this.start();

    // 如果当前有不同类型的 Block，先关闭
    if (this.currentBlockType && this.currentBlockType !== type) {
      this.closeCurrentBlock();
    }

    // 检查是否已有该类型的 Block
    if (type === 'text' && this.textBlockIndex !== null) {
      return this.textBlockIndex;
    }
    if (type === 'thinking' && this.thinkingBlockIndex !== null) {
      return this.thinkingBlockIndex;
    }

    // 开启新 Block
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

  /**
   * 关闭当前活动 Block
   */
  closeCurrentBlock() {
    if (this.currentBlockType === 'text') {
      this.closeTextBlock();
    } else if (this.currentBlockType === 'thinking') {
      this.closeThinkingBlock();
    }
    this.currentBlockType = null;
  }

  /**
   * 关闭文本 Block
   */
  closeTextBlock() {
    if (this.textBlockIndex === null) return;
    const index = this.textBlockIndex;
    this.textBlockIndex = null;
    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  /**
   * 关闭思考 Block
   */
  closeThinkingBlock() {
    if (this.thinkingBlockIndex === null) return;
    const index = this.thinkingBlockIndex;
    this.thinkingBlockIndex = null;
    this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
  }

  /**
   * 发送文本内容
   */
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

  /**
   * 发送思考内容
   */
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

  /**
   * 发送签名（用于 thinking block）
   * @param {string} signature
   */
  sendSignature(signature) {
    if (!signature || this.finished) return;

    // 确保 thinking block 存在
    if (this.thinkingBlockIndex === null) {
      this.ensureBlock('thinking');
    }

    this.writeEvent('content_block_delta', {
      type: 'content_block_delta',
      index: this.thinkingBlockIndex,
      delta: { type: 'signature_delta', signature }
    });
  }

  /**
   * 发送工具调用
   */
  sendToolCalls(toolCalls) {
    if (!toolCalls || toolCalls.length === 0 || this.finished) return;

    // 关闭当前所有 Block
    this.closeCurrentBlock();

    for (const call of toolCalls) {
      const index = this.nextIndex++;
      const args = call?.function?.arguments ?? '{}';
      const inputJson = typeof args === 'string' ? args : JSON.stringify(args);

      this.trackTokens(inputJson);

      // Block 开始
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

      // 分块发送 JSON
      const CHUNK_SIZE = 128;
      for (let i = 0; i < inputJson.length; i += CHUNK_SIZE) {
        const chunk = inputJson.slice(i, i + CHUNK_SIZE);
        this.writeEvent('content_block_delta', {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: chunk }
        });
      }

      // Block 结束
      this.writeEvent('content_block_stop', { type: 'content_block_stop', index });
    }
  }

  /**
   * 完成响应
   * @param {object} usage - Token 使用统计
   * @param {string} stopReason - Claude 格式停止原因 (end_turn, tool_use, max_tokens 等)
   * @param {object} extraUsage - 额外的 usage 字段（如 cache 相关）
   */
  finish(usage, stopReason = 'end_turn', extraUsage = null) {
    if (this.finished) return;
    this.finished = true;

    // 关闭所有 Block
    this.closeCurrentBlock();

    const finalUsage = {
      ...this.buildUsage(usage),
      cache_creation_input_tokens: usage?.cache_creation_input_tokens ?? 0,
      cache_read_input_tokens: usage?.cache_read_input_tokens ?? usage?.prompt_tokens_details?.cached_tokens ?? 0,
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
