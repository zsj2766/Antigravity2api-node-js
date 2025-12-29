/**
 * Anthropic/Claude ↔ Gemini 完整适配器
 *
 * 职责：
 * 1. 请求转换：Claude Messages API → Antigravity/Gemini 格式
 * 2. 响应转换：ClaudeSseEmitter 类处理 SSE 流式响应
 * 3. Token 计算辅助函数
 */

import config from '../../config/config.js';
import { generateRequestId, generateToolUseId } from '../idGenerator.js';
import {
  isThinkingModel,
  generateGenerationConfig,
  getThoughtSignature,
  getTextThoughtSignature,
  safeJsonParse,
  safeJsonStringify
} from '../utils.js';
import {
  convertClaudeImageToGemini,
  convertClaudeDocumentToGemini,
  extractMediaFromToolResult
} from './imageUtils.js';
import { cleanJsonSchema } from './schemaUtils.js';
import { mapGeminiStopReason } from './stopReasonMapper.js';
import { writeSSE, buildMessageStartPayload, convertToolCallsToClaudeBlocks, buildClaudeContentBlocks, countClaudeTokens } from './sseUtils.js';
import { estimateTokensFromText } from './tokenUtils.js';
import { SignatureManager, rememberToolThoughtSignature } from './signatureManager.js';
import {
  createWebSearchState,
  extractGroundingData,
  hasGroundingData,
  emitWebSearchBlocksForEmitter,
  resolveWebSearchRedirectUrls,
  toWebSearchResults,
  buildCitationFromSupport,
  makeSrvToolUseId
} from './webSearchEmitter.js';
import { maybeInjectMcpHintIntoSystemText, hasMcpTools, filterMcpTools } from '../../mcp/claudeTransformerMcp.js';

// ==================== 请求转换：Claude → Gemini ====================

// 正则常量
const INVOKE_REGEX = /<invoke\b[^>]*>[\s\S]*?<\/invoke>/gi;
const TOOL_RESULT_REGEX = /<tool_result\b[^>]*>[\s\S]*?<\/tool_result>/gi;

/**
 * 解析 Claude 消息内容块，提取各类型内容
 */
function parseClaudeContentBlocks(content) {
  const result = {
    textParts: [],
    thinkingParts: [],
    toolCalls: [],
    toolResults: [],
    images: [],
    documents: []
  };

  if (typeof content === 'string') {
    // 字符串内容，清理可能的 XML 标签
    const cleanedText = content
      .replace(INVOKE_REGEX, '')
      .replace(TOOL_RESULT_REGEX, '');
    if (cleanedText.trim()) {
      result.textParts.push(cleanedText);
    }
    return result;
  }

  if (!Array.isArray(content)) {
    return result;
  }

  for (const block of content) {
    if (!block || typeof block !== 'object') continue;

    switch (block.type) {
      case 'text':
        const cleanedText = (block.text || '')
          .replace(INVOKE_REGEX, '')
          .replace(TOOL_RESULT_REGEX, '');
        if (cleanedText.trim()) {
          result.textParts.push(cleanedText);
        }
        break;

      case 'thinking':
        // 保留完整的 thinking 块（含 signature）
        if (block.thinking) {
          result.thinkingParts.push({
            thinking: block.thinking,
            signature: block.signature
          });
        }
        break;

      case 'redacted_thinking':
        // 被隐藏的思考内容，只保留签名数据用于验证
        // 不转换内容，但保留元数据以便后续处理
        if (block.data) {
          result.thinkingParts.push({
            thinking: '[redacted]',
            signature: block.data,
            redacted: true
          });
        }
        break;

      case 'tool_use':
        result.toolCalls.push({
          id: block.id || generateToolUseId(),
          name: block.name,
          input: block.input || {}
        });
        break;

      case 'tool_result':
        // 解析 tool_result 中可能嵌套的图片和文档
        const mediaContent = extractMediaFromToolResult(block.content);
        result.toolResults.push({
          tool_use_id: block.tool_use_id,
          content: mediaContent.text || '',
          images: mediaContent.images || [],
          documents: mediaContent.documents || [],
          is_error: block.is_error
        });
        break;

      case 'image':
        const geminiImage = convertClaudeImageToGemini(block);
        if (geminiImage) {
          result.images.push(geminiImage);
        }
        break;

      case 'document':
        const geminiDoc = convertClaudeDocumentToGemini(block);
        if (geminiDoc) {
          result.documents.push(geminiDoc);
        }
        break;
    }
  }

  return result;
}

/**
 * 处理 Claude user 消息，转换为 Gemini 格式
 */
function handleClaudeUserMessage(parsed, antigravityMessages, enableThinking) {
  // 先处理 tool_result（作为 user role 发送 functionResponse）
  for (const toolResult of parsed.toolResults) {
    // 查找对应的 functionCall 名称
    let functionName = '';
    for (let i = antigravityMessages.length - 1; i >= 0; i--) {
      if (antigravityMessages[i].role === 'model') {
        const parts = antigravityMessages[i].parts;
        for (const part of parts) {
          if (part?.functionCall?.id === toolResult.tool_use_id) {
            functionName = part.functionCall.name;
            break;
          }
        }
        if (functionName) break;
      }
    }

    // 构建 functionResponse，包含嵌套的图片和文档
    // 处理 is_error 标志，添加统一前缀
    let outputContent = toolResult.content;
    if (toolResult.is_error) {
      outputContent = `Error: ${outputContent}`;
    }

    const responseParts = [
      {
        functionResponse: {
          id: toolResult.tool_use_id,
          name: functionName,
          response: {
            output: outputContent
          }
        }
      }
    ];

    // 添加嵌套的图片
    if (toolResult.images && toolResult.images.length > 0) {
      responseParts.push(...toolResult.images);
    }

    // 添加嵌套的文档
    if (toolResult.documents && toolResult.documents.length > 0) {
      responseParts.push(...toolResult.documents);
    }

    const lastMessage = antigravityMessages[antigravityMessages.length - 1];
    if (lastMessage?.role === 'user' && lastMessage.parts.some(p => p.functionResponse)) {
      lastMessage.parts.push(...responseParts);
    } else {
      antigravityMessages.push({
        role: 'user',
        parts: responseParts
      });
    }
  }

  // 处理文本、图片和文档内容
  const hasContent = parsed.textParts.length > 0 || parsed.images.length > 0 || parsed.documents.length > 0;
  if (hasContent) {
    const parts = [];

    // 添加文本
    if (parsed.textParts.length > 0) {
      let text = parsed.textParts.join('\n');
      // 如果启用 thinking，追加提示
      // 注意：这是 Gemini thinking 模式的 Prompt 注入 hack 方案
      // 适用于 gemini-2.0-flash-thinking / gemini-3-pro 等支持 interleaved thinking 的模型
      // 未来模型更新后可能需要调整为官方 config.thinking_config 方式
      if (enableThinking) {
        text += '<thinking_mode>interleaved</thinking_mode><max_thinking_length>16000</max_thinking_length>';
      }
      parts.push({ text });
    }

    // 添加图片
    parts.push(...parsed.images);

    // 添加文档
    parts.push(...parsed.documents);

    if (parts.length > 0) {
      antigravityMessages.push({
        role: 'user',
        parts
      });
    }
  }
}

/**
 * 处理 Claude assistant 消息，转换为 Gemini 格式
 */
function handleClaudeAssistantMessage(parsed, antigravityMessages, modelName) {
  const allowThoughtSignature = typeof modelName === 'string' && modelName.includes('gemini-3');
  const parts = [];

  // 处理 thinking 块
  for (const thinkingBlock of parsed.thinkingParts) {
    // thinking 内容作为带 thoughtSignature 的文本
    if (thinkingBlock.thinking && thinkingBlock.signature && allowThoughtSignature) {
      parts.push({
        text: thinkingBlock.thinking,
        thoughtSignature: thinkingBlock.signature
      });
    }
  }

  // 处理文本内容
  if (parsed.textParts.length > 0) {
    const contentText = parsed.textParts.join('\n');
    const textThoughtSignature = allowThoughtSignature ? getTextThoughtSignature(contentText) : undefined;

    const textPart = { text: textThoughtSignature?.text ?? contentText };
    if (allowThoughtSignature && textThoughtSignature?.signature) {
      textPart.thoughtSignature = textThoughtSignature.signature;
    }
    parts.push(textPart);
  }

  // 处理 tool_use
  for (const toolCall of parsed.toolCalls) {
    const thoughtSignature = getThoughtSignature(toolCall.id);
    const part = {
      functionCall: {
        id: toolCall.id,
        name: toolCall.name,
        args: typeof toolCall.input === 'string' ? safeJsonParse(toolCall.input) : toolCall.input
      }
    };
    if (thoughtSignature) {
      part.thoughtSignature = thoughtSignature;
    }
    parts.push(part);
  }

  // 合并到上一条 model 消息（如果只有 functionCall 且上一条是 model）
  const lastMessage = antigravityMessages[antigravityMessages.length - 1];
  const onlyToolCalls = parts.every(p => p.functionCall) && parts.length > 0;

  if (lastMessage?.role === 'model' && onlyToolCalls && parsed.textParts.length === 0) {
    lastMessage.parts.push(...parts);
  } else if (parts.length > 0) {
    antigravityMessages.push({
      role: 'model',
      parts
    });
  }
}

/**
 * 将 Claude Messages 数组直接转换为 Gemini Contents 格式
 */
function claudeMessagesToGemini(claudeMessages, modelName, enableThinking = false) {
  const antigravityMessages = [];

  for (const message of claudeMessages) {
    const parsed = parseClaudeContentBlocks(message.content);

    if (message.role === 'user') {
      handleClaudeUserMessage(parsed, antigravityMessages, enableThinking);
    } else if (message.role === 'assistant') {
      handleClaudeAssistantMessage(parsed, antigravityMessages, modelName);
    }
  }

  return antigravityMessages;
}

/**
 * 提取 Claude 请求中的 system prompt
 */
function extractSystemPrompt(claudeSystem) {
  if (!claudeSystem) return '';

  if (Array.isArray(claudeSystem)) {
    return claudeSystem
      .map(block => {
        if (typeof block === 'string') return block;
        if (block && typeof block === 'object' && 'text' in block) {
          return block.text || '';
        }
        return '';
      })
      .join('\n');
  }

  return claudeSystem;
}

/**
 * 将 Claude 工具定义转换为 Antigravity/Gemini 格式
 * Claude 格式: { name, description, input_schema }
 */
function convertClaudeToolsToAntigravity(tools) {
  if (!tools || !Array.isArray(tools) || tools.length === 0) return [];

  return tools.map((tool) => {
    if (!tool || typeof tool !== 'object') return null;

    const name = tool.name;
    const description = tool.description;
    const rawParameters = tool.input_schema || {};

    // 深拷贝避免修改原始数据
    const parameters = rawParameters ? JSON.parse(JSON.stringify(rawParameters)) : {};

    // 清理 JSON Schema
    const cleanedParameters = cleanJsonSchema(parameters);

    return {
      functionDeclarations: [
        {
          name,
          description,
          parameters: cleanedParameters
        }
      ]
    };
  }).filter(Boolean);
}

/**
 * 将 Claude tool_choice 转换为 Gemini functionCallingConfig
 * Claude: {type: "auto"/"any"/"tool"/"none", name?: string}
 * Gemini: {mode: "AUTO"/"ANY"/"NONE", allowed_function_names?: [...]}
 */
function mapClaudeToolChoiceToGemini(toolChoice) {
  if (!toolChoice) {
    return { mode: "AUTO" };
  }

  switch (toolChoice.type) {
    case 'auto':
      return { mode: "AUTO" };
    case 'any':
      return { mode: "ANY" };
    case 'tool':
      return {
        mode: "ANY",
        allowed_function_names: [toolChoice.name]
      };
    case 'none':
      return { mode: "NONE" };
    default:
      return { mode: "AUTO" };
  }
}

/**
 * 从 Claude 请求体生成完整的 Antigravity 请求体
 *
 * @param {object} claudeBody - Claude Messages API 请求体
 * @param {object} token - Token 信息（包含 projectId, sessionId）
 * @returns {object} - Antigravity 请求体
 */
function generateRequestBodyFromAnthropic(claudeBody, token) {
  // 验证必填参数
  if (!claudeBody || typeof claudeBody !== 'object') {
    throw new Error('请求体格式不合法');
  }
  if (typeof claudeBody.max_tokens !== 'number' || Number.isNaN(claudeBody.max_tokens)) {
    throw new Error('max_tokens 是必填数字');
  }
  if (!Array.isArray(claudeBody.messages) || claudeBody.messages.length === 0) {
    throw new Error('messages 不能为空');
  }

  const modelName = claudeBody.model;
  const enableThinking = claudeBody.thinking?.type === 'enabled' || isThinkingModel(modelName);

  // 直接将 Claude Messages 转换为 Gemini Contents
  const contents = claudeMessagesToGemini(claudeBody.messages, modelName, enableThinking);

  // 处理 system prompt
  const systemText = extractSystemPrompt(claudeBody.system);
  const fullSystemText = systemText
    ? `${systemText}\n\n${config.systemInstruction}`
    : config.systemInstruction;

  // 生成参数配置
  const parameters = {
    temperature: claudeBody.temperature ?? 0.2,
    top_p: claudeBody.top_p ?? 1,
    max_tokens: claudeBody.max_tokens,
    thinking: claudeBody.thinking
  };

  // 构建 toolConfig
  const hasTools = claudeBody.tools && claudeBody.tools.length > 0;
  const functionCallingConfig = hasTools
    ? mapClaudeToolChoiceToGemini(claudeBody.tool_choice)
    : { mode: "NONE" };

  // 构建请求体
  return {
    project: token.projectId,
    requestId: generateRequestId(),
    request: {
      contents,
      systemInstruction: {
        role: "user",
        parts: [{ text: fullSystemText }]
      },
      tools: convertClaudeToolsToAntigravity(claudeBody.tools),
      toolConfig: {
        functionCallingConfig
      },
      generationConfig: generateGenerationConfig(parameters, enableThinking, modelName),
      sessionId: token.sessionId
    },
    model: modelName,
    userAgent: "antigravity"
  };
}

// ==================== ClaudeSseEmitter 类 ====================

class ClaudeSseEmitter {
  constructor(res, requestId, { model, inputTokens } = {}) {
    this.res = res;
    this.requestId = requestId || generateRequestId();
    this.model = model || 'claude-proxy';
    this.inputTokens = inputTokens || 0;
    this.nextIndex = 0;
    this.textBlockIndex = null;
    this.thinkingBlockIndex = null;
    this.finished = false;
    this.hasStarted = false;
    this.totalOutputTokens = 0;
    // 新增：签名管理器
    this.signatures = new SignatureManager();
    this.trailingSignature = null;
    this.hasThinking = false;
    // 新增：web search 状态
    this.webSearchMode = false;
    this.webSearch = createWebSearchState();
  }

  start() {
    if (this.hasStarted) return;
    this.hasStarted = true;
    writeSSE(this.res, 'message_start', buildMessageStartPayload(this.requestId, this.model, this.inputTokens));
  }

  ensureTextBlock() {
    if (!this.hasStarted) this.start();
    if (this.textBlockIndex !== null) return;
    this.textBlockIndex = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.textBlockIndex,
      content_block: { type: 'text', text: '' }
    });
  }

  ensureThinkingBlock() {
    if (!this.hasStarted) this.start();
    if (this.thinkingBlockIndex !== null) return;
    this.thinkingBlockIndex = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index: this.thinkingBlockIndex,
      content_block: { type: 'thinking', thinking: '', signature: '' }
    });
  }

  sendText(text) {
    if (!text) return;
    this.closeThinkingBlock();
    this.ensureTextBlock();
    this.totalOutputTokens += estimateTokensFromText(text);
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: this.textBlockIndex,
      delta: { type: 'text_delta', text }
    });
  }

  sendThinking(thinking) {
    if (!thinking) return;
    this.closeTextBlock();
    this.ensureThinkingBlock();
    this.hasThinking = true;
    this.totalOutputTokens += estimateTokensFromText(thinking);
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: this.thinkingBlockIndex,
      delta: { type: 'thinking_delta', thinking }
    });
  }

  sendSignature(signature) {
    if (!signature) return;
    this.ensureThinkingBlock();
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index: this.thinkingBlockIndex,
      delta: { type: 'signature_delta', signature }
    });
  }

  storeSignature(signature) {
    this.signatures.store(signature);
  }

  async sendToolCalls(toolCalls = []) {
    if (!toolCalls || toolCalls.length === 0) return;
    await this.closeTextBlock();
    await this.closeThinkingBlock();

    toolCalls.forEach(call => {
      const index = this.nextIndex++;
      const args = call?.function?.arguments ?? '{}';
      const inputJson = typeof args === 'string' ? args : JSON.stringify(args);
      this.totalOutputTokens += estimateTokensFromText(inputJson);

      const toolUseBlock = {
        type: 'tool_use',
        id: call.id || generateToolUseId(),
        name: call?.function?.name || 'tool',
        input: {}
      };

      if (call.signature) {
        toolUseBlock.signature = call.signature;
        rememberToolThoughtSignature(toolUseBlock.id, call.signature);
      }

      writeSSE(this.res, 'content_block_start', {
        type: 'content_block_start',
        index,
        content_block: toolUseBlock
      });
      const CHUNK_SIZE = 128;
      for (let i = 0; i < inputJson.length; i += CHUNK_SIZE) {
        const chunk = inputJson.slice(i, i + CHUNK_SIZE);
        writeSSE(this.res, 'content_block_delta', {
          type: 'content_block_delta',
          index,
          delta: { type: 'input_json_delta', partial_json: chunk }
        });
      }
      writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
    });
  }

  async closeTextBlock() {
    if (this.textBlockIndex === null) return;
    const index = this.textBlockIndex;
    this.textBlockIndex = null;
    writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
  }

  async closeThinkingBlock() {
    if (this.thinkingBlockIndex === null) return;
    if (this.signatures.hasPending()) {
      this.sendSignature(this.signatures.consume());
    }
    const index = this.thinkingBlockIndex;
    this.thinkingBlockIndex = null;
    writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
  }

  emitTrailingSignature() {
    if (!this.trailingSignature || !this.hasThinking) {
      this.trailingSignature = null;
      return;
    }
    const index = this.nextIndex++;
    writeSSE(this.res, 'content_block_start', {
      type: 'content_block_start',
      index,
      content_block: { type: 'thinking', thinking: '', signature: '' }
    });
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index,
      delta: { type: 'thinking_delta', thinking: '' }
    });
    writeSSE(this.res, 'content_block_delta', {
      type: 'content_block_delta',
      index,
      delta: { type: 'signature_delta', signature: this.trailingSignature }
    });
    writeSSE(this.res, 'content_block_stop', { type: 'content_block_stop', index });
    this.trailingSignature = null;
  }

  finish(usage, stopReason = null, extraUsage = null) {
    if (this.finished) return;
    this.finished = true;
    this.closeTextBlock();
    this.closeThinkingBlock();
    this.emitTrailingSignature();

    const outputTokens = usage?.completion_tokens ?? usage?.output_tokens ?? (this.totalOutputTokens ?? 0);
    const inputTokens = usage?.prompt_tokens ?? usage?.input_tokens ?? (this.inputTokens ?? null);
    const cacheCreationTokens = usage?.cache_creation_input_tokens ?? 0;
    const cacheReadTokens = usage?.cache_read_input_tokens ?? usage?.prompt_tokens_details?.cached_tokens ?? 0;
    const finalStopReason = stopReason || 'end_turn';

    const finalUsage = {
      input_tokens: inputTokens || 0,
      output_tokens: outputTokens || 0,
      cache_creation_input_tokens: cacheCreationTokens,
      cache_read_input_tokens: cacheReadTokens,
      ...(extraUsage || {})
    };

    writeSSE(this.res, 'message_delta', {
      type: 'message_delta',
      delta: { stop_reason: finalStopReason, stop_sequence: null },
      usage: finalUsage
    });
    writeSSE(this.res, 'message_stop', { type: 'message_stop' });
    this.res.end();
  }
}

// ==================== 导出 ====================

export {
  // 请求转换
  generateRequestBodyFromAnthropic,
  claudeMessagesToGemini,
  extractSystemPrompt,
  convertClaudeToolsToAntigravity,
  parseClaudeContentBlocks,
  handleClaudeUserMessage,
  handleClaudeAssistantMessage,
  // 响应转换
  ClaudeSseEmitter,
  // 新增：Gemini → Claude 辅助函数
  convertGeminiPartsToClaude,
  convertGeminiResponseToClaude
};

// 从 common/sseUtils.js 再导出以保持 API 兼容性
export { countClaudeTokens, estimateTokensFromText, buildClaudeContentBlocks, convertToolCallsToClaudeBlocks };

// ==================== Gemini → Claude 辅助转换 ====================

/**
 * 将 Gemini parts 转换为 Claude 内容块数组
 * @param {Array} parts - Gemini parts 数组
 * @returns {Array} - Claude 内容块数组
 */
function convertGeminiPartsToClaude(parts) {
  if (!Array.isArray(parts)) {
    return [];
  }

  const blocks = [];

  for (const part of parts) {
    if (!part) continue;

    // 思考内容 → Claude thinking 块
    if (part.thought === true && part.text) {
      blocks.push({
        type: 'thinking',
        thinking: part.text
        // 注意：不伪造 signature，避免客户端校验失败
      });
    }
    // 普通文本
    else if (part.text !== undefined) {
      blocks.push({
        type: 'text',
        text: part.text
      });
    }
    // 函数调用 → Claude tool_use 块
    else if (part.functionCall) {
      blocks.push({
        type: 'tool_use',
        id: part.functionCall.id || generateToolUseId(),
        name: part.functionCall.name,
        input: part.functionCall.args || {}
      });
    }
    // 内联数据（图片）→ Claude image 块
    else if (part.inlineData && part.inlineData.mimeType?.startsWith('image/')) {
      blocks.push({
        type: 'image',
        source: {
          type: 'base64',
          media_type: part.inlineData.mimeType,
          data: part.inlineData.data
        }
      });
    }
    // 内联数据（文档）→ Claude document 块
    else if (part.inlineData) {
      blocks.push({
        type: 'document',
        source: {
          type: 'base64',
          media_type: part.inlineData.mimeType,
          data: part.inlineData.data
        }
      });
    }
    // 文件数据 → Claude 对应块
    else if (part.fileData) {
      if (part.fileData.mimeType?.startsWith('image/')) {
        blocks.push({
          type: 'image',
          source: {
            type: 'url',
            url: part.fileData.fileUri
          }
        });
      } else {
        blocks.push({
          type: 'document',
          source: {
            type: 'url',
            url: part.fileData.fileUri,
            media_type: part.fileData.mimeType
          }
        });
      }
    }
  }

  return blocks;
}

/**
 * 将 Gemini 完整响应转换为 Claude 格式
 * @param {object} geminiResponse - Gemini 响应对象
 * @param {string} requestId - 请求 ID
 * @param {string} model - 模型名称
 * @returns {object} - Claude 格式响应
 */
function convertGeminiResponseToClaude(geminiResponse, requestId, model) {
  const candidate = geminiResponse?.candidates?.[0];
  const parts = candidate?.content?.parts || [];
  const content = convertGeminiPartsToClaude(parts);

  // 映射 finishReason（使用统一映射模块）
  const finishReason = candidate?.finishReason;
  const hasToolUse = content.some(b => b.type === 'tool_use');
  const stopReason = mapGeminiStopReason(finishReason, hasToolUse).claude;

  // 转换 usage (包含缓存统计字段)
  const usageMetadata = geminiResponse?.usageMetadata;
  const usage = {
    input_tokens: usageMetadata?.promptTokenCount ?? usageMetadata?.inputTokenCount ?? 0,
    output_tokens: usageMetadata?.candidatesTokenCount ?? usageMetadata?.outputTokenCount ?? 0,
    cache_creation_input_tokens: 0, // Gemini 无对应字段，Context Caching 通过显式 API 创建
    cache_read_input_tokens: usageMetadata?.cachedContentTokenCount ?? 0
  };

  return {
    id: `msg_${requestId || generateRequestId()}`,
    type: 'message',
    role: 'assistant',
    model: model || 'gemini-proxy',
    content,
    stop_reason: stopReason,
    stop_sequence: null,
    usage
  };
}
