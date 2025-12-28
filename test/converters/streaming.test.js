/**
 * 转换器单元测试 - 流式响应解析
 *
 * 覆盖场景：
 * - 增量文本拼接（多个 text_delta 合并为完整文本）
 * - 工具调用增量组装（input_json_delta 拼接为完整 JSON）
 * - 并行工具调用（多个工具同时增量返回）
 * - SSE 事件序列完整性验证
 * - Claude 格式事件序列
 * - OpenAI 格式事件序列
 * - Thinking 模式测试
 * - 异常容错测试
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';

// Gemini -> OpenAI 解析器
import {
  parseGeminiStreamToOpenAI,
  flushTextAccumulator
} from '../../src/utils/converters/openaiAdapter.js';

// Claude SSE 发射器
import { ClaudeSseEmitter } from '../../src/utils/converters/anthropicAdapter.js';

// OpenAI SSE 发射器
import { OpenAISseEmitter } from '../../src/utils/converters/openaiToClaudeAdapter.js';

// ==================== Mock 工具类 ====================

/**
 * Mock HTTP Response 对象
 * 用于捕获 SSE 输出并解析事件
 */
class MockResponse {
  constructor() {
    this.chunks = [];
    this.ended = false;
  }

  write(chunk) {
    this.chunks.push(chunk);
  }

  end() {
    this.ended = true;
  }

  getOutput() {
    return this.chunks.join('');
  }

  /**
   * 解析 Claude SSE 事件（event: + data: 格式）
   * @returns {Array} 事件数组 [{event, data}, ...]
   */
  getClaudeEvents() {
    const output = this.getOutput();
    const events = [];
    const lines = output.split('\n');

    let currentEvent = null;
    for (const line of lines) {
      if (line.startsWith('event: ')) {
        currentEvent = line.slice(7).trim();
      } else if (line.startsWith('data: ') && currentEvent) {
        try {
          const data = JSON.parse(line.slice(6));
          events.push({ event: currentEvent, data });
        } catch (e) {
          // 忽略解析错误
        }
        currentEvent = null;
      }
    }
    return events;
  }

  /**
   * 解析 OpenAI SSE 事件（data: 格式）
   * @returns {Array} 数据数组
   */
  getOpenAIEvents() {
    const output = this.getOutput();
    const events = [];
    const lines = output.split('\n');

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const payload = line.slice(6).trim();
        if (payload === '[DONE]') {
          events.push({ done: true });
        } else {
          try {
            events.push(JSON.parse(payload));
          } catch (e) {
            // 忽略解析错误
          }
        }
      }
    }
    return events;
  }
}

/**
 * 创建初始化的 Gemini 解析状态对象
 */
function createGeminiState() {
  return {
    toolCalls: [],
    toolCallIndex: 0,
    usage: null,
    textAccumulator: { text: '', signature: null },
    finishReason: null,
    reasoningId: null
  };
}

/**
 * 构造 Gemini SSE 行
 */
function buildGeminiSSELine(parts, usageMetadata = null, finishReason = null) {
  const response = {
    candidates: [{
      content: { parts }
    }]
  };
  if (finishReason) {
    response.candidates[0].finishReason = finishReason;
  }
  if (usageMetadata) {
    response.usageMetadata = usageMetadata;
  }
  return `data: ${JSON.stringify({ response })}`;
}

// ==================== 1. parseGeminiStreamToOpenAI 测试 ====================

describe('parseGeminiStreamToOpenAI', () => {
  test('multiple text parts accumulate correctly', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    // 模拟多个文本增量
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: 'Hello' }]),
      state,
      callback
    );
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: ' World' }]),
      state,
      callback
    );
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: '!' }]),
      state,
      callback
    );

    // 验证回调收到 3 个 text 事件
    assert.strictEqual(events.length, 3);
    assert.deepStrictEqual(events[0], { type: 'text', content: 'Hello' });
    assert.deepStrictEqual(events[1], { type: 'text', content: ' World' });
    assert.deepStrictEqual(events[2], { type: 'text', content: '!' });

    // 验证累积器状态
    assert.strictEqual(state.textAccumulator.text, 'Hello World!');
  });

  test('function call triggers tool_call_chunk event', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{
        functionCall: {
          name: 'get_weather',
          args: { location: 'Tokyo' }
        }
      }]),
      state,
      callback
    );

    // 验证收到 tool_call_chunk 事件
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].type, 'tool_call_chunk');
    assert.strictEqual(events[0].index, 0);
    assert.strictEqual(events[0].tool_call.function.name, 'get_weather');

    // 验证索引递增
    assert.strictEqual(state.toolCallIndex, 1);
  });

  test('parallel function calls have incrementing indices', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    // 第一个工具调用
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{
        functionCall: { name: 'tool_a', args: { a: 1 } }
      }]),
      state,
      callback
    );

    // 第二个工具调用
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{
        functionCall: { name: 'tool_b', args: { b: 2 } }
      }]),
      state,
      callback
    );

    assert.strictEqual(events.length, 2);
    assert.strictEqual(events[0].index, 0);
    assert.strictEqual(events[0].tool_call.function.name, 'tool_a');
    assert.strictEqual(events[1].index, 1);
    assert.strictEqual(events[1].tool_call.function.name, 'tool_b');
  });

  test('thinking/reasoning part triggers reasoning event', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{
        thought: true,
        text: 'Let me think about this...'
      }]),
      state,
      callback
    );

    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].type, 'reasoning');
    assert.ok(events[0].id);
    assert.deepStrictEqual(events[0].summary, [{ type: 'summary_text', text: 'Let me think about this...' }]);
  });

  test('finish reason triggers finish_reason event', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: 'Done' }], null, 'STOP'),
      state,
      callback
    );

    // 应该有 text 事件和 finish_reason 事件
    assert.strictEqual(events.length, 2);
    assert.strictEqual(events[0].type, 'text');
    assert.strictEqual(events[1].type, 'finish_reason');
    assert.strictEqual(events[1].finishReason, 'stop');
  });

  test('malformed JSON line is safely ignored', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    // 格式错误的 JSON
    parseGeminiStreamToOpenAI('data: { truncated...', state, callback);

    // 不应该有任何事件
    assert.strictEqual(events.length, 0);

    // 后续正常数据仍能解析
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: 'Valid' }]),
      state,
      callback
    );
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].content, 'Valid');
  });

  test('non-SSE line is ignored', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI('not a data line', state, callback);
    parseGeminiStreamToOpenAI('', state, callback);
    parseGeminiStreamToOpenAI('event: something', state, callback);

    assert.strictEqual(events.length, 0);
  });

  test('usage metadata is captured in state', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: 'Hi' }], {
        promptTokenCount: 100,
        candidatesTokenCount: 50,
        totalTokenCount: 150
      }),
      state,
      callback
    );

    assert.ok(state.usage);
    assert.strictEqual(state.usage.prompt_tokens, 100);
    assert.strictEqual(state.usage.completion_tokens, 50);
  });
});

// ==================== 2. ClaudeSseEmitter 测试 ====================

describe('ClaudeSseEmitter', () => {
  test('start() emits message_start event', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req-123', {
      model: 'claude-3',
      inputTokens: 100
    });

    emitter.start();

    const events = res.getClaudeEvents();
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].event, 'message_start');
    assert.strictEqual(events[0].data.type, 'message_start');
    assert.strictEqual(events[0].data.message.id, 'msg_test-req-123');
    assert.strictEqual(events[0].data.message.model, 'claude-3');
    assert.strictEqual(events[0].data.message.usage.input_tokens, 100);
  });

  test('sendText() emits content_block_start and content_block_delta', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.sendText('Hello');
    emitter.sendText(' World');

    const events = res.getClaudeEvents();

    // 应该有: message_start, content_block_start, delta, delta
    assert.strictEqual(events.length, 4);
    assert.strictEqual(events[0].event, 'message_start');
    assert.strictEqual(events[1].event, 'content_block_start');
    assert.strictEqual(events[1].data.content_block.type, 'text');
    assert.strictEqual(events[2].event, 'content_block_delta');
    assert.strictEqual(events[2].data.delta.type, 'text_delta');
    assert.strictEqual(events[2].data.delta.text, 'Hello');
    assert.strictEqual(events[3].event, 'content_block_delta');
    assert.strictEqual(events[3].data.delta.text, ' World');

    // 验证 index 保持一致
    assert.strictEqual(events[2].data.index, events[3].data.index);
  });

  test('sendThinking() emits thinking_delta', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.sendThinking('Let me think...');

    const events = res.getClaudeEvents();

    // message_start, content_block_start (thinking), content_block_delta
    assert.strictEqual(events.length, 3);
    assert.strictEqual(events[1].data.content_block.type, 'thinking');
    assert.strictEqual(events[2].data.delta.type, 'thinking_delta');
    assert.strictEqual(events[2].data.delta.thinking, 'Let me think...');
  });

  test('sendToolCalls() emits tool_use blocks with chunked input_json_delta', async () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });
    emitter.start();

    // 构造超过 128 字符的 JSON
    const longArgs = JSON.stringify({
      query: 'A'.repeat(150),
      options: { debug: true, verbose: true }
    });

    await emitter.sendToolCalls([{
      id: 'call_123',
      function: {
        name: 'search',
        arguments: longArgs
      }
    }]);

    const events = res.getClaudeEvents();

    // 找到所有 input_json_delta 事件
    const jsonDeltas = events.filter(e =>
      e.event === 'content_block_delta' &&
      e.data.delta?.type === 'input_json_delta'
    );

    // 应该有多个 delta（超过 128 字符会分片）
    assert.ok(jsonDeltas.length > 1, `Expected multiple JSON deltas, got ${jsonDeltas.length}`);

    // 拼接所有 partial_json
    const reassembled = jsonDeltas.map(e => e.data.delta.partial_json).join('');
    assert.strictEqual(reassembled, longArgs);

    // 验证拼接后的 JSON 是合法的
    assert.doesNotThrow(() => JSON.parse(reassembled));
  });

  test('parallel tool calls have separate indices', async () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });
    emitter.start();

    await emitter.sendToolCalls([
      { id: 'call_1', function: { name: 'tool_a', arguments: '{}' } },
      { id: 'call_2', function: { name: 'tool_b', arguments: '{}' } }
    ]);

    const events = res.getClaudeEvents();

    // 找到 content_block_start 事件
    const blockStarts = events.filter(e => e.event === 'content_block_start');
    const toolBlocks = blockStarts.filter(e => e.data.content_block?.type === 'tool_use');

    assert.strictEqual(toolBlocks.length, 2);
    assert.strictEqual(toolBlocks[0].data.content_block.name, 'tool_a');
    assert.strictEqual(toolBlocks[1].data.content_block.name, 'tool_b');

    // 验证索引不同
    assert.notStrictEqual(toolBlocks[0].data.index, toolBlocks[1].data.index);
  });

  test('finish() emits message_delta and message_stop', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.start();
    emitter.sendText('Done');
    emitter.finish({ output_tokens: 10 }, 'end_turn');

    const events = res.getClaudeEvents();
    const lastTwo = events.slice(-2);

    assert.strictEqual(lastTwo[0].event, 'message_delta');
    assert.strictEqual(lastTwo[0].data.delta.stop_reason, 'end_turn');
    assert.strictEqual(lastTwo[0].data.usage.output_tokens, 10);

    assert.strictEqual(lastTwo[1].event, 'message_stop');
    assert.strictEqual(lastTwo[1].data.type, 'message_stop');

    assert.ok(res.ended);
  });

  test('complete Claude event sequence', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.start();
    emitter.sendText('Hello');
    emitter.finish({}, 'end_turn');

    const events = res.getClaudeEvents();
    const eventTypes = events.map(e => e.event);

    // 验证完整事件序列
    assert.deepStrictEqual(eventTypes, [
      'message_start',
      'content_block_start',
      'content_block_delta',
      'content_block_stop',
      'message_delta',
      'message_stop'
    ]);
  });

  test('thinking to text transition closes thinking block', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.sendThinking('Thinking...');
    emitter.sendText('Answer');

    const events = res.getClaudeEvents();

    // 找到 content_block_stop 事件
    const stops = events.filter(e => e.event === 'content_block_stop');

    // 应该有一个 stop（thinking 块被关闭）
    assert.strictEqual(stops.length, 1);

    // 验证 stop 的 index 匹配 thinking 块
    const thinkingStart = events.find(e =>
      e.event === 'content_block_start' &&
      e.data.content_block.type === 'thinking'
    );
    assert.strictEqual(stops[0].data.index, thinkingStart.data.index);
  });

  test('empty text is ignored', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    emitter.sendText('');
    emitter.sendText(null);
    emitter.sendText(undefined);

    // 不应该有任何事件
    assert.strictEqual(res.chunks.length, 0);
  });
});

// ==================== 3. OpenAISseEmitter 测试 ====================

describe('OpenAISseEmitter', () => {
  test('start() emits first chunk with role:assistant', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();

    const events = res.getOpenAIEvents();
    assert.strictEqual(events.length, 1);
    assert.strictEqual(events[0].object, 'chat.completion.chunk');
    assert.strictEqual(events[0].choices[0].delta.role, 'assistant');
  });

  test('sendTextDelta() emits delta.content', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.sendTextDelta('Hello');
    emitter.sendTextDelta(' World');

    const events = res.getOpenAIEvents();

    // 首个 chunk (自动 start) + 2 个 text delta
    assert.strictEqual(events.length, 3);
    assert.strictEqual(events[1].choices[0].delta.content, 'Hello');
    assert.strictEqual(events[2].choices[0].delta.content, ' World');
  });

  test('tool call sequence with start/arguments/finish', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();
    emitter.sendToolCallStart('call_123', 'get_weather');
    emitter.sendToolCallArgumentsDelta('{"loc');
    emitter.sendToolCallArgumentsDelta('ation":"NYC"}');
    emitter.finishToolCall();

    const events = res.getOpenAIEvents();

    // 找到 tool_calls 事件
    const toolEvents = events.filter(e => e.choices?.[0]?.delta?.tool_calls);

    assert.strictEqual(toolEvents.length, 3);

    // 第一个包含 id 和 name
    assert.strictEqual(toolEvents[0].choices[0].delta.tool_calls[0].id, 'call_123');
    assert.strictEqual(toolEvents[0].choices[0].delta.tool_calls[0].function.name, 'get_weather');

    // 后续只有 arguments
    assert.strictEqual(toolEvents[1].choices[0].delta.tool_calls[0].function.arguments, '{"loc');
    assert.strictEqual(toolEvents[2].choices[0].delta.tool_calls[0].function.arguments, 'ation":"NYC"}');
  });

  test('parallel tool calls have incrementing indices', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();
    emitter.sendToolCallStart('call_1', 'tool_a');
    emitter.sendToolCallArgumentsDelta('{}');
    emitter.finishToolCall();
    emitter.sendToolCallStart('call_2', 'tool_b');
    emitter.sendToolCallArgumentsDelta('{}');
    emitter.finishToolCall();

    const events = res.getOpenAIEvents();
    const toolEvents = events.filter(e => e.choices?.[0]?.delta?.tool_calls);

    // 第一个工具 index=0
    const tool1Events = toolEvents.filter(e =>
      e.choices[0].delta.tool_calls[0].index === 0
    );
    // 第二个工具 index=1
    const tool2Events = toolEvents.filter(e =>
      e.choices[0].delta.tool_calls[0].index === 1
    );

    assert.ok(tool1Events.length > 0);
    assert.ok(tool2Events.length > 0);
  });

  test('finish() emits finish_reason and [DONE]', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();
    emitter.sendTextDelta('Hi');
    emitter.finish('stop', { input_tokens: 10, output_tokens: 5 });

    const events = res.getOpenAIEvents();
    const lastTwo = events.slice(-2);

    // 倒数第二个有 finish_reason
    assert.strictEqual(lastTwo[0].choices[0].finish_reason, 'stop');
    assert.strictEqual(lastTwo[0].usage.prompt_tokens, 10);
    assert.strictEqual(lastTwo[0].usage.completion_tokens, 5);

    // 最后一个是 [DONE]
    assert.deepStrictEqual(lastTwo[1], { done: true });

    assert.ok(res.ended);
  });

  test('complete OpenAI event sequence', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();
    emitter.sendTextDelta('Hello');
    emitter.finish('stop');

    const events = res.getOpenAIEvents();

    // 验证序列: 首个 chunk -> content delta -> finish_reason -> [DONE]
    assert.strictEqual(events.length, 4);
    assert.strictEqual(events[0].choices[0].delta.role, 'assistant');
    assert.strictEqual(events[1].choices[0].delta.content, 'Hello');
    assert.strictEqual(events[2].choices[0].finish_reason, 'stop');
    assert.deepStrictEqual(events[3], { done: true });
  });

  test('empty text is ignored', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.sendTextDelta('');
    emitter.sendTextDelta(null);

    // 不应该有任何事件
    assert.strictEqual(res.chunks.length, 0);
  });

  test('finished emitter ignores further calls', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.start();
    emitter.finish('stop');

    const countBefore = res.chunks.length;
    emitter.sendTextDelta('Should be ignored');

    assert.strictEqual(res.chunks.length, countBefore);
  });
});

// ==================== 4. 边界情况和容错测试 ====================

describe('Edge Cases and Error Handling', () => {
  test('ClaudeSseEmitter auto-starts on sendText', () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });

    // 不手动调用 start()
    emitter.sendText('Hello');

    const events = res.getClaudeEvents();

    // 应该自动发送 message_start
    assert.strictEqual(events[0].event, 'message_start');
  });

  test('OpenAISseEmitter auto-starts on sendTextDelta', () => {
    const res = new MockResponse();
    const emitter = new OpenAISseEmitter(res, 'test-req', { model: 'gpt-4' });

    emitter.sendTextDelta('Hello');

    const events = res.getOpenAIEvents();

    // 首个事件应该有 role: assistant
    assert.strictEqual(events[0].choices[0].delta.role, 'assistant');
  });

  test('ClaudeSseEmitter handles multibyte characters in JSON chunking', async () => {
    const res = new MockResponse();
    const emitter = new ClaudeSseEmitter(res, 'test-req', { model: 'claude-3' });
    emitter.start();

    // 包含中文的 JSON（测试字符分片不会破坏字符）
    const argsWithChinese = JSON.stringify({
      message: '你好世界'.repeat(30)  // 超过 128 字符
    });

    await emitter.sendToolCalls([{
      id: 'call_cn',
      function: {
        name: 'translate',
        arguments: argsWithChinese
      }
    }]);

    const events = res.getClaudeEvents();
    const jsonDeltas = events.filter(e =>
      e.event === 'content_block_delta' &&
      e.data.delta?.type === 'input_json_delta'
    );

    // 拼接并验证
    const reassembled = jsonDeltas.map(e => e.data.delta.partial_json).join('');
    assert.strictEqual(reassembled, argsWithChinese);

    // 验证 JSON 合法且内容正确
    const parsed = JSON.parse(reassembled);
    assert.strictEqual(parsed.message, '你好世界'.repeat(30));
  });

  test('flushTextAccumulator clears accumulator state', () => {
    const state = createGeminiState();
    state.textAccumulator.text = 'Some text';
    state.textAccumulator.signature = 'sig123';

    flushTextAccumulator(state);

    assert.strictEqual(state.textAccumulator.text, '');
    assert.strictEqual(state.textAccumulator.signature, null);
  });

  test('parseGeminiStreamToOpenAI handles empty parts array', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([]),
      state,
      callback
    );

    assert.strictEqual(events.length, 0);
  });

  test('parseGeminiStreamToOpenAI processes valid parts', () => {
    const state = createGeminiState();
    const events = [];
    const callback = (data) => events.push(data);

    // 测试正常的 parts 数组
    parseGeminiStreamToOpenAI(
      buildGeminiSSELine([{ text: 'First' }, { text: 'Second' }]),
      state,
      callback
    );

    assert.strictEqual(events.length, 2);
    assert.strictEqual(events[0].content, 'First');
    assert.strictEqual(events[1].content, 'Second');
  });
});
