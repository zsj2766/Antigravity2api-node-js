/**
 * 【响应转换】OpenAI 协议 SSE Emitter
 *
 * 输出格式: OpenAI Chat Completions API SSE
 *
 * 实现 OpenAI Chat Completions API 的 SSE 协议格式：
 * - 扁平 delta 流（无显式 block 生命周期）
 * - data: {...}\n\n 格式
 * - 结束标记 data: [DONE]
 *
 * 子类：ClaudeToOpenAISseEmitter
 */

import { BaseSseEmitter } from './BaseSseEmitter.js';
import { generateToolCallId } from '../../idGenerator.js';

export class OpenAIProtocolEmitter extends BaseSseEmitter {
  constructor(res, options = {}) {
    super(res, options);

    // 工具调用索引
    this.toolCallIndex = 0;
  }

  /**
   * 写入 OpenAI SSE 数据
   * @param {object} data - 数据对象
   */
  writeData(data) {
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  /**
   * 构建 chunk 基础结构
   */
  buildChunkBase() {
    return {
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model
    };
  }

  /**
   * 启动 SSE 流（发送首个 chunk，包含 role）
   */
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

  /**
   * 发送文本内容
   */
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

  /**
   * 发送思考内容
   * OpenAI 不支持 thinking，忽略或转为普通文本（可配置）
   */
  sendThinking(thinking) {
    // OpenAI 标准协议不支持 thinking
    // 可选：转为 reasoning_content（部分模型支持）
    // 当前策略：忽略
  }

  /**
   * 发送工具调用开始
   * @param {string} id - 工具调用 ID
   * @param {string} name - 函数名
   */
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

  /**
   * 发送工具调用参数增量
   * @param {string} args - 参数 JSON 字符串片段
   */
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

  /**
   * 完成当前工具调用，准备下一个
   */
  finishToolCall() {
    this.toolCallIndex++;
  }

  /**
   * 发送完整工具调用（非增量模式）
   */
  sendToolCalls(toolCalls) {
    if (!toolCalls || toolCalls.length === 0 || this.finished) return;
    if (!this.hasStarted) this.start();

    for (const call of toolCalls) {
      const id = call.id || generateToolCallId();
      const name = call?.function?.name || 'tool';
      const args = call?.function?.arguments || '{}';
      const argsStr = typeof args === 'string' ? args : JSON.stringify(args);

      this.trackTokens(argsStr);

      // 发送开始
      this.sendToolCallStart(id, name);

      // 发送参数
      this.sendToolCallArguments(argsStr);

      // 完成当前工具调用
      this.finishToolCall();
    }
  }

  /**
   * 完成响应
   * @param {object} usage - Token 使用统计
   * @param {string} finishReason - OpenAI 格式 (stop, length, tool_calls 等)
   */
  finish(usage, finishReason = 'stop') {
    if (this.finished) return;
    this.finished = true;

    const finalUsage = this.buildUsage(usage);

    // 发送最终 chunk
    this.writeData({
      ...this.buildChunkBase(),
      choices: [{
        index: 0,
        delta: {},
        finish_reason: finishReason
      }],
      usage: {
        prompt_tokens: finalUsage.input_tokens,
        completion_tokens: finalUsage.output_tokens,
        total_tokens: finalUsage.input_tokens + finalUsage.output_tokens
      }
    });

    // 发送 [DONE] 标记
    this.res.write('data: [DONE]\n\n');
    this.res.end();
  }
}

export default OpenAIProtocolEmitter;
