/**
 * Claude → OpenAI 格式转换适配器
 *
 * 职责：
 * 1. 请求转换：Claude Messages API → OpenAI Chat Completions API
 * 2. 响应转换：Claude 流式响应 → OpenAI SSE 格式
 * 3. 工具调用格式转换
 *
 * 支持的 Claude 内容类型：
 * - text: 纯文本
 * - image (base64/url): 图片
 * - document (base64/url): PDF/CSV 等文档
 * - tool_use: 工具调用 → OpenAI tool_calls（原生格式）
 * - tool_result: 工具结果 → OpenAI tool 消息（原生格式）
 * - thinking: 扩展思考
 * - redacted_thinking: 隐藏的思考内容
 */

import { generateRequestId, generateToolCallId } from '../idGenerator.js';
import { convertClaudeImageToOpenAI, extractMediaFromToolResult } from './imageUtils.js';
import { resolveReasoningEffort } from './thinkingConfig.js';
import { mapClaudeStopToOpenAI } from './stopReasonMapper.js';
import { convertToolCallsToClaudeBlocks, buildClaudeContentBlocks, countClaudeTokens } from './sseUtils.js';
import { estimateTokensFromText } from './tokenUtils.js';
import { safeJsonStringify, safeJsonParse } from '../utils.js';

// ==================== 请求转换：Claude → OpenAI ====================

/**
 * 将 Claude tool_use 块转换为 OpenAI tool_calls 格式
 * @param {Array} blocks - Claude 内容块数组
 * @returns {Array} - OpenAI tool_calls 数组
 */
export function extractToolUsesAsOpenAIToolCalls(blocks) {
  if (!Array.isArray(blocks)) return [];

  return blocks
    .filter(b => b && b.type === 'tool_use')
    .map(b => ({
      id: b.id || generateToolCallId(),
      type: 'function',
      function: {
        name: b.name || 'unknown',
        arguments: safeJsonStringify(b.input) || '{}'
      }
    }));
}

/**
 * 将 Claude 内容块转换为 OpenAI 格式的内容数组
 * 支持多模态内容（文本+图片）
 * 注意：tool_use 不再转为 XML，而是单独提取为 tool_calls
 */
export function convertClaudeContentToOpenAI(content) {
  // 字符串内容直接返回
  if (typeof content === 'string') {
    return { content: content, toolCalls: [] };
  }

  if (!Array.isArray(content)) {
    return { content: '', toolCalls: [] };
  }

  const parts = [];
  let hasMultimodal = false;
  const toolCalls = extractToolUsesAsOpenAIToolCalls(content);

  for (const block of content) {
    if (!block || typeof block !== 'object') continue;

    switch (block.type) {
      case 'text':
        if (block.text && block.text.trim()) {
          parts.push({ type: 'text', text: block.text });
        }
        break;

      case 'thinking':
        // OpenAI 不支持 thinking，历史对话中的 thinking 内容不传递
        break;

      case 'redacted_thinking':
        // OpenAI 不支持 thinking，直接忽略
        break;

      case 'tool_use':
        // 不再转为 XML，已通过 extractToolUsesAsOpenAIToolCalls 处理
        break;

      case 'tool_result':
        // tool_result 将在消息级别处理，转为单独的 tool 消息
        // 这里只提取可能的嵌套图片
        const mediaContent = extractMediaFromToolResult(block.content);
        if (mediaContent.images && mediaContent.images.length > 0) {
          hasMultimodal = true;
          for (const img of mediaContent.images) {
            if (img.inlineData) {
              parts.push({
                type: 'image_url',
                image_url: {
                  url: `data:${img.inlineData.mimeType};base64,${img.inlineData.data}`,
                  detail: 'auto'
                }
              });
            }
          }
        }
        break;

      case 'image':
        hasMultimodal = true;
        const openaiImage = convertClaudeImageToOpenAI(block);
        if (openaiImage) {
          parts.push(openaiImage);
        }
        break;

      case 'document':
        // 将 Claude document 转换为 OpenAI file 格式
        hasMultimodal = true;
        const docSource = block.source;
        if (docSource) {
          const mediaType = docSource.media_type || 'application/pdf';
          const filename = block.title || `document.${mediaType.split('/')[1] || 'pdf'}`;

          if (docSource.type === 'base64' && docSource.data) {
            // base64 数据转换为 OpenAI file 格式
            parts.push({
              type: 'file',
              file: {
                filename: filename,
                file_data: `data:${mediaType};base64,${docSource.data}`
              }
            });
          } else if (docSource.type === 'url' && docSource.url) {
            // URL 类型的文档
            parts.push({
              type: 'file',
              file: {
                filename: filename,
                file_data: docSource.url
              }
            });
          }
        }
        break;
    }
  }

  // 构建最终内容
  let finalContent;
  if (hasMultimodal) {
    finalContent = parts;
  } else if (parts.length === 0) {
    finalContent = '';
  } else {
    finalContent = parts
      .filter(p => p.type === 'text')
      .map(p => p.text)
      .join('\n');
  }

  return { content: finalContent, toolCalls };
}

/**
 * 从 Claude 消息中提取 tool_result 块
 */
function extractToolResults(content) {
  if (!Array.isArray(content)) return [];

  return content
    .filter(b => b && b.type === 'tool_result')
    .map(b => ({
      tool_use_id: b.tool_use_id,
      content: typeof b.content === 'string' ? b.content : JSON.stringify(b.content || ''),
      is_error: b.is_error
    }));
}

/**
 * 映射 Claude 角色到 OpenAI 角色
 */
function mapClaudeRole(role) {
  return role === 'assistant' ? 'assistant' : 'user';
}

/**
 * 将 Claude 请求体转换为 OpenAI 格式
 * 重构版：使用原生 tool_calls 替代 XML hack
 */
export function mapClaudeToOpenAI(body, triggerSignal) {
  if (!body || typeof body !== 'object') {
    throw new Error('请求体格式不合法');
  }
  if (typeof body.max_tokens !== 'number' || Number.isNaN(body.max_tokens)) {
    throw new Error('max_tokens 是必填数字');
  }
  if (!Array.isArray(body.messages) || body.messages.length === 0) {
    throw new Error('messages 不能为空');
  }

  const messages = [];

  // 处理 system 消息
  if (body.system) {
    const systemContent = Array.isArray(body.system)
      ? body.system
          .map(block => {
            if (typeof block === 'string') return block;
            if (block && typeof block === 'object' && 'text' in block) {
              return block.text || '';
            }
            return '';
          })
          .join('\n')
      : body.system;
    messages.push({ role: 'system', content: systemContent });
  }

  // 处理消息
  for (const message of body.messages) {
    if (message.role === 'user') {
      // 检查是否包含 tool_result
      const toolResults = extractToolResults(message.content);

      if (toolResults.length > 0) {
        // 将 tool_result 转为 OpenAI tool 消息
        for (const tr of toolResults) {
          let content = tr.content;
          // 处理 is_error，添加统一前缀
          if (tr.is_error) {
            content = `Error: ${content}`;
          }
          messages.push({
            role: 'tool',
            tool_call_id: tr.tool_use_id,
            content: content
          });
        }

        // 如果还有其他内容（非 tool_result），添加为用户消息
        const { content } = convertClaudeContentToOpenAI(message.content);
        if (content && (typeof content === 'string' ? content.trim() : content.length > 0)) {
          messages.push({
            role: 'user',
            content
          });
        }
      } else {
        // 普通用户消息
        const { content } = convertClaudeContentToOpenAI(message.content);

        messages.push({
          role: 'user',
          content
        });
      }
    } else if (message.role === 'assistant') {
      // 助手消息：提取内容和工具调用
      const { content, toolCalls } = convertClaudeContentToOpenAI(message.content);

      const assistantMsg = {
        role: 'assistant',
        content: content || null
      };

      // 添加工具调用（原生格式）
      if (toolCalls.length > 0) {
        assistantMsg.tool_calls = toolCalls;
      }

      messages.push(assistantMsg);
    }
  }

  const result = {
    model: body.model,
    stream: body.stream !== false,
    temperature: body.temperature ?? 0.2,
    top_p: body.top_p ?? 1,
    max_tokens: body.max_tokens,
    messages
  };

  // 处理 thinking -> reasoning_effort
  // 让目标服务决定是否支持，转换器不做模型限制
  if (body.thinking && body.thinking.type === 'enabled' && body.thinking.budget_tokens) {
    result.reasoning_effort = resolveReasoningEffort(body.thinking.budget_tokens);
  }

  // 添加工具定义
  if (body.tools && body.tools.length > 0) {
    result.tools = mapClaudeToolsToOpenAITools(body.tools);
    result.tool_choice = mapClaudeToolChoiceToOpenAI(body.tool_choice);
  }

  return result;
}

/**
 * 将 Claude tool_choice 转换为 OpenAI 格式
 * Claude: {type: "auto"/"any"/"tool"/"none", name?: string}
 * OpenAI: "auto"/"required"/"none" 或 {type: "function", function: {name: string}}
 */
export function mapClaudeToolChoiceToOpenAI(toolChoice) {
  if (!toolChoice) return 'auto';

  switch (toolChoice.type) {
    case 'auto':
      return 'auto';
    case 'any':
      return 'required';
    case 'tool':
      return { type: 'function', function: { name: toolChoice.name } };
    case 'none':
      return 'none';
    default:
      return 'auto';
  }
}

/**
 * 将 Claude 工具定义转换为 OpenAI 格式
 */
export function mapClaudeToolsToOpenAITools(tools = []) {
  if (!Array.isArray(tools)) return [];
  return tools.map(tool => ({
    type: 'function',
    function: {
      name: tool?.name,
      description: tool?.description,
      parameters: tool?.input_schema || {}
    }
  }));
}

// ==================== 响应转换：Claude → OpenAI（非流式）====================

// 从 common/sseUtils.js 再导出以保持 API 兼容性
export { convertToolCallsToClaudeBlocks, estimateTokensFromText, countClaudeTokens, buildClaudeContentBlocks };

/**
 * 将 Claude tool_use 块转换为 OpenAI tool_calls
 * @param {Array} blocks - Claude 内容块数组
 * @returns {Array} - OpenAI tool_calls 数组
 */
function convertClaudeToolUsesToOpenAI(blocks) {
  if (!Array.isArray(blocks)) return [];

  return blocks
    .filter(b => b.type === 'tool_use')
    .map(b => ({
      id: b.id,
      type: 'function',
      function: {
        name: b.name,
        arguments: safeJsonStringify(b.input, '{}')
      }
    }));
}

/**
 * 从 Claude 内容块中提取文本
 * @param {Array} blocks - Claude 内容块数组
 * @returns {string} - 合并后的文本
 */
function extractTextFromClaudeBlocks(blocks) {
  if (!Array.isArray(blocks)) return '';

  return blocks
    .filter(b => b.type === 'text')
    .map(b => b.text || '')
    .join('');
}

/**
 * 将 Claude 非流式响应转换为 OpenAI 格式
 * @param {object} claudeResponse - Claude 响应
 * @param {string} requestId - 请求 ID
 * @returns {object} - OpenAI 格式响应
 */
export function convertClaudeResponseToOpenAI(claudeResponse, requestId) {
  const content = claudeResponse.content || [];
  const text = extractTextFromClaudeBlocks(content);
  const toolCalls = convertClaudeToolUsesToOpenAI(content);

  const message = {
    role: 'assistant',
    content: text || null
  };

  if (toolCalls.length > 0) {
    message.tool_calls = toolCalls;
  }

  // 映射 stop_reason（使用统一映射模块）
  const finishReason = mapClaudeStopToOpenAI(claudeResponse.stop_reason);

  return {
    id: `chatcmpl-${requestId || generateRequestId()}`,
    object: 'chat.completion',
    created: Math.floor(Date.now() / 1000),
    model: claudeResponse.model || 'claude-proxy',
    choices: [{
      index: 0,
      message,
      finish_reason: finishReason
    }],
    usage: {
      prompt_tokens: claudeResponse.usage?.input_tokens || 0,
      completion_tokens: claudeResponse.usage?.output_tokens || 0,
      total_tokens: (claudeResponse.usage?.input_tokens || 0) + (claudeResponse.usage?.output_tokens || 0)
    }
  };
}

// ==================== SSE 流式响应转换 ====================

/**
 * Claude → OpenAI SSE 响应发射器类
 * 用于将 Claude 流式响应转换为 OpenAI SSE 格式
 */
export class ClaudeToOpenAISseEmitter {
  constructor(res, requestId, { model } = {}) {
    this.res = res;
    this.requestId = requestId || generateRequestId();
    this.model = model || 'claude-proxy';
    this.finished = false;
    this.hasStarted = false;
    this.toolCallIndex = 0;
    this.currentToolCallId = null;
  }

  /**
   * 发送首个 chunk (包含 role: assistant)
   */
  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: { role: 'assistant', content: '' },
        finish_reason: null
      }]
    });
  }

  writeSSE(data) {
    this.res.write(`data: ${JSON.stringify(data)}\n\n`);
  }

  /**
   * 发送文本增量
   */
  sendTextDelta(text) {
    if (!text || this.finished) return;

    // 容错机制：自动补发首个 chunk
    if (!this.hasStarted) this.start();

    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: { content: text },
        finish_reason: null
      }]
    });
  }

  /**
   * 发送工具调用开始
   */
  sendToolCallStart(id, name) {
    if (this.finished) return;

    this.currentToolCallId = id;
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: {
          tool_calls: [{
            index: this.toolCallIndex,
            id: id,
            type: 'function',
            function: { name: name, arguments: '' }
          }]
        },
        finish_reason: null
      }]
    });
  }

  /**
   * 发送工具调用参数增量
   */
  sendToolCallArgumentsDelta(args) {
    if (this.finished) return;

    const argsStr = typeof args === 'string' ? args : JSON.stringify(args);
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
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
    this.currentToolCallId = null;
  }

  /**
   * 完成响应
   */
  finish(finishReason = 'stop', usage = null) {
    if (this.finished) return;
    this.finished = true;

    // 发送最终 chunk
    this.writeSSE({
      id: `chatcmpl-${this.requestId}`,
      object: 'chat.completion.chunk',
      created: Math.floor(Date.now() / 1000),
      model: this.model,
      choices: [{
        index: 0,
        delta: {},
        finish_reason: finishReason
      }],
      usage: usage ? {
        prompt_tokens: usage.input_tokens || usage.prompt_tokens || 0,
        completion_tokens: usage.output_tokens || usage.completion_tokens || 0,
        total_tokens: (usage.input_tokens || usage.prompt_tokens || 0) + (usage.output_tokens || usage.completion_tokens || 0)
      } : undefined
    });

    // 发送 [DONE]
    this.res.write('data: [DONE]\n\n');
    this.res.end();
  }
}
