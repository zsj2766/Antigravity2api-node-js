/**
 * 【响应转换】SSE Emitter 基类
 *
 * 职责：
 * - I/O 管理（写入响应流）
 * - 状态管理（started, finished）
 * - Token 统计
 *
 * 子类需实现具体协议格式
 */

import { generateRequestId } from '../../idGenerator.js';
import { estimateTokensFromText } from '../tokenUtils.js';

export class BaseSseEmitter {
  constructor(res, options = {}) {
    this.res = res;
    this.requestId = options.requestId || generateRequestId();
    this.model = options.model || 'proxy';
    this.inputTokens = options.inputTokens || 0;

    // 状态标志
    this.hasStarted = false;
    this.finished = false;

    // Token 统计
    this.totalOutputTokens = 0;
  }

  /**
   * 估算并累加 Token
   * @param {string} text - 文本内容
   */
  trackTokens(text) {
    if (text) {
      this.totalOutputTokens += estimateTokensFromText(text);
    }
  }

  /**
   * 构建最终 usage 对象
   * @param {object} usage - 外部传入的 usage（可选）
   * @returns {object}
   */
  buildUsage(usage) {
    return {
      input_tokens: usage?.input_tokens ?? usage?.prompt_tokens ?? this.inputTokens,
      output_tokens: usage?.output_tokens ?? usage?.completion_tokens ?? this.totalOutputTokens
    };
  }

  /**
   * 结束响应流
   */
  end() {
    if (this.finished) return;
    this.finished = true;
    this.res.end();
  }

  // ==================== 子类必须实现 ====================

  /**
   * 启动 SSE 流（发送首个事件）
   */
  start() {
    throw new Error('start() must be implemented by subclass');
  }

  /**
   * 发送文本内容
   * @param {string} text
   */
  sendText(text) {
    throw new Error('sendText() must be implemented by subclass');
  }

  /**
   * 发送思考内容
   * @param {string} thinking
   */
  sendThinking(thinking) {
    throw new Error('sendThinking() must be implemented by subclass');
  }

  /**
   * 发送工具调用
   * @param {Array} toolCalls
   */
  sendToolCalls(toolCalls) {
    throw new Error('sendToolCalls() must be implemented by subclass');
  }

  /**
   * 完成响应
   * @param {object} usage - Token 使用统计
   * @param {string} stopReason - 停止原因
   */
  finish(usage, stopReason) {
    throw new Error('finish() must be implemented by subclass');
  }
}

export default BaseSseEmitter;
