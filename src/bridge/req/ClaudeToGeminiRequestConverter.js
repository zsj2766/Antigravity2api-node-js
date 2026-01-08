/**
 * Claude → Gemini 请求转换器
 *
 * 输入: Claude Messages API 请求格式
 * 输出: Gemini API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import {
  cleanJsonSchema,
  generateToolUseId,
  mergeConsecutiveRoles,
  normalizeThinkingBudget,
  shouldUseThinkingLevel,
  ANTIGRAVITY_SYSTEM_PREFIX
} from '../common/index.js';
import { isThinkingModel, getThoughtSignature, getTextThoughtSignature, safeJsonParse } from '../../utils/utils.js';

// 注意：已移除 INVOKE_REGEX 和 TOOL_RESULT_REGEX
// 原因：全局正则替换会误删用户讨论 XML 或工具调用格式的正常文本内容
// Bridge 层不应擅自修改用户输入

export class ClaudeToGeminiRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   *
   * @param {object} body - Claude Messages API 请求体
   * @param {object} context - 上下文信息
   * @returns {Promise<object>} Gemini API 请求体
   */
  async convert(body, context = {}) {
    if (!body || typeof body !== 'object') {
      throw new Error('请求体格式不合法');
    }
    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      throw new Error('messages 不能为空');
    }

    const { messages, system, tools, tool_choice, thinking, ...parameters } = body;
    const modelName = context.model || body.model || 'gemini-3-pro';
    const enableThinking = isThinkingModel(modelName);

    const contents = this.convertMessages(messages, modelName, enableThinking);
    const geminiTools = this.convertTools(tools);
    // 传入 modelName 以支持 VALIDATED 模式判断
    const toolConfig = this.convertToolConfig(tool_choice, tools, modelName);

    // 提取系统指令
    const systemInstruction = this.extractSystemInstruction(system);

    // [Antigravity] 注入身份前缀 (模拟 CLIProxyAPI 行为)
    const finalSystemInstruction = this.maybeInjectAntigravityPrefix(systemInstruction, modelName);

    // 确保消息角色交替（Gemini 强制要求 User/Model 交替）
    const mergedContents = mergeConsecutiveRoles(contents);
    if (mergedContents.length === 0) {
      mergedContents.push({ role: 'user', parts: [{ text: '' }] });
    } else if (mergedContents[0].role !== 'user') {
      mergedContents.unshift({ role: 'user', parts: [{ text: '' }] });
    }

    // 构建请求体
    const generationConfig = this.buildGenerationConfig(parameters, modelName);

    // 处理 thinking 配置 -> Gemini thinkingConfig
    // 参考：CLIProxyAPI normalizeAntigravityThinking
    if (thinking && thinking.type === 'enabled') {
      // 判断是否使用 thinkingLevel（Gemini 3 系列）还是 thinkingBudget（其他模型）
      const useLevel = shouldUseThinkingLevel(modelName);

      if (useLevel) {
        // Gemini 3 系列使用 thinkingLevel (LOW/MEDIUM/HIGH)
        // Claude thinking.thinking_level 直接映射
        generationConfig.thinkingConfig = {
          thinkingLevel: thinking.thinking_level?.toUpperCase() || 'HIGH',
          includeThoughts: true
        };
      } else {
        // 其他模型使用 thinkingBudget (数值)
        const rawBudget = thinking.budget_tokens || 8192;
        const maxOutputTokens = generationConfig.maxOutputTokens || 0;

        // 规范化 budget：应用 min/max 限制和 Claude 的 budget < maxOutputTokens 约束
        const normalizedBudget = normalizeThinkingBudget(modelName, rawBudget, maxOutputTokens);

        if (normalizedBudget === null) {
          // budget 低于最小值，不启用 thinking
          // 这里不添加 thinkingConfig
        } else {
          generationConfig.thinkingConfig = {
            thinkingBudget: normalizedBudget,
            includeThoughts: true
          };
        }
      }
    }

    const requestBody = {
      contents: mergedContents,
      generationConfig
    };

    if (finalSystemInstruction) {
      requestBody.systemInstruction = { parts: [{ text: finalSystemInstruction }] };
    }

    if (geminiTools.length > 0) {
      requestBody.tools = geminiTools;
      requestBody.toolConfig = { functionCallingConfig: toolConfig };
    }

    return requestBody;
  }

  /**
   * 转换消息数组 (Claude → Gemini)
   *
   * @param {Array} messages - Claude 消息数组
   * @param {string} modelName - 模型名称
   * @param {boolean} enableThinking - 是否启用思考模式
   * @returns {Array} Gemini contents 数组
   */
  convertMessages(messages, modelName, enableThinking = false) {
    if (!Array.isArray(messages)) return [];

    const contents = [];

    for (const message of messages) {
      const parsed = this.convertContent(message.content);

      if (message.role === 'user') {
        // 传递原始内容以保持顺序
        this.handleUserMessage(parsed, contents, enableThinking, message.content);
      } else if (message.role === 'assistant') {
        this.handleAssistantMessage(parsed, contents, modelName, message.content);
      }
    }

    return contents;
  }

  /**
   * 转换内容块 (Claude content → 解析结构)
   *
   * 注意：返回包含分类后各种内容的对象，用于后续构建 Gemini parts
   *
   * @param {string|Array} content - Claude 内容（字符串或内容块数组）
   * @returns {{ textParts, thinkingParts, toolCalls, toolResults, images, documents }} 分类后的内容
   */
  convertContent(content) {
    const result = {
      textParts: [],
      thinkingParts: [],
      toolCalls: [],
      toolResults: [],
      images: [],
      documents: []
    };

    if (typeof content === 'string') {
      // 保留用户原始文本，不进行任何清理
      if (content.trim()) {
        result.textParts.push(content);
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
          // 保留用户原始文本，不进行任何清理
          if (block.text?.trim()) {
            result.textParts.push(block.text);
          }
          break;

        case 'thinking':
          // 保留 signature-only thinking 块（thinking 为空但有 signature）
          if (block.thinking !== undefined || block.signature) {
            result.thinkingParts.push({
              thinking: block.thinking ?? '',
              signature: block.signature
            });
          }
          break;

        case 'redacted_thinking':
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
          const mediaContent = this.extractToolResultMedia(block.content);
          result.toolResults.push({
            tool_use_id: block.tool_use_id,
            content: mediaContent.text || '',
            images: mediaContent.images || [],
            documents: mediaContent.documents || [],
            is_error: block.is_error
          });
          break;

        case 'image':
          const geminiImage = this.convertImage(block);
          if (geminiImage) {
            result.images.push(geminiImage);
          }
          break;

        case 'document':
          const geminiDoc = this.convertDocument(block);
          if (geminiDoc) {
            result.documents.push(geminiDoc);
          } else if (block?.source?.type === 'content' && Array.isArray(block.source.content)) {
            // content 类型文档：递归处理嵌套内容
            const nestedResult = this.convertContent(block.source.content);
            if (nestedResult.textParts && nestedResult.textParts.length > 0) {
              result.textParts.push(...nestedResult.textParts);
            }
            result.images.push(...nestedResult.images);
            result.documents.push(...nestedResult.documents);
          }
          break;
      }
    }

    return result;
  }

  /**
   * 从 tool_result 内容中提取媒体（图片、文档）
   *
   * @param {string|Array} content - tool_result 的 content 字段
   * @returns {{ text: string, images: Array, documents: Array }} 分类后的媒体内容
   */
  extractToolResultMedia(content) {
    const result = { text: '', images: [], documents: [] };

    if (typeof content === 'string') {
      result.text = content;
      return result;
    }

    if (!Array.isArray(content)) {
      result.text = typeof content === 'object' ? JSON.stringify(content) : String(content ?? '');
      return result;
    }

    for (const block of content) {
      if (!block || typeof block !== 'object') continue;

      switch (block.type) {
        case 'text':
          result.text += (result.text ? '\n' : '') + (block.text || '');
          break;

        case 'image': {
          const img = this.convertImage(block);
          if (img) result.images.push(img);
          break;
        }

        case 'document': {
          const source = block?.source;
          if (!source) break;

          if (source.type === 'text' && source.data) {
            result.text += (result.text ? '\n' : '') + source.data;
            break;
          }

          if (source.type === 'content' && Array.isArray(source.content)) {
            const nested = this.extractToolResultMedia(source.content);
            if (nested.text) {
              result.text += (result.text ? '\n' : '') + nested.text;
            }
            if (nested.images?.length) {
              result.images.push(...nested.images);
            }
            if (nested.documents?.length) {
              result.documents.push(...nested.documents);
            }
            break;
          }

          const doc = this.convertDocument(block);
          if (doc) result.documents.push(doc);
          break;
        }

        default:
          break;
      }
    }

    return result;
  }

  /**
   * 处理 user 消息，构建 Gemini contents
   *
   * 注意：保持内容的原始顺序，避免破坏交错内容的上下文关系
   *
   * @param {object} parsed - 解析后的内容结构
   * @param {Array} contents - Gemini contents 数组（会被修改）
   * @param {boolean} enableThinking - 是否启用思考模式
   * @param {string|Array} originalContent - 原始内容（用于保持顺序）
   */
  handleUserMessage(parsed, contents, enableThinking, originalContent = null) {
    // 如果有原始内容数组，按原始顺序处理（保持所有内容类型的交错顺序）
    if (originalContent && Array.isArray(originalContent)) {
      this.buildOrderedUserMessage(originalContent, parsed.toolResults, contents);
    } else if (typeof originalContent === 'string' && originalContent.trim()) {
      // 字符串内容
      contents.push({ role: 'user', parts: [{ text: originalContent }] });
    } else {
      // 兜底：先处理 tool_result，再处理其他内容（向后兼容）
      for (const toolResult of parsed.toolResults) {
        const { parts } = this.convertToolResult(toolResult, contents);
        const lastMessage = contents[contents.length - 1];
        if (lastMessage?.role === 'user' && lastMessage.parts.some(p => p.functionResponse)) {
          lastMessage.parts.push(...parts);
        } else {
          contents.push({ role: 'user', parts });
        }
      }

      const hasContent = parsed.textParts.length > 0 || parsed.images.length > 0 || parsed.documents.length > 0;
      if (hasContent) {
        const parts = [];
        if (parsed.textParts.length > 0) {
          let text = parsed.textParts.join('\n');
          parts.push({ text });
        }
        parts.push(...parsed.images);
        parts.push(...parsed.documents);
        if (parts.length > 0) {
          contents.push({ role: 'user', parts });
        }
      }
    }
  }

  /**
   * 按原始顺序构建 user 消息（保持所有内容类型的交错顺序）
   *
   * 策略：遍历原始内容，将连续的非 tool_result 内容合并，遇到 tool_result 时切分消息
   *
   * @param {Array} originalContent - Claude 原始内容数组
   * @param {Array} parsedToolResults - 解析后的 tool_result 数组（包含提取的媒体）
   * @param {Array} contents - Gemini contents 数组（会被修改）
   */
  buildOrderedUserMessage(originalContent, parsedToolResults, contents) {
    // 构建 tool_use_id -> parsedToolResult 的映射
    const toolResultMap = new Map();
    for (const tr of parsedToolResults) {
      toolResultMap.set(tr.tool_use_id, tr);
    }

    let currentParts = [];

    const flushCurrentParts = () => {
      if (currentParts.length > 0) {
        contents.push({ role: 'user', parts: currentParts });
        currentParts = [];
      }
    };

    for (const block of originalContent) {
      if (!block || typeof block !== 'object') continue;

      if (block.type === 'tool_result') {
        // 遇到 tool_result，先 flush 当前积累的 parts
        flushCurrentParts();

        // 处理 tool_result
        const parsedResult = toolResultMap.get(block.tool_use_id);
        if (parsedResult) {
          const { parts } = this.convertToolResult(parsedResult, contents);
          const lastMessage = contents[contents.length - 1];
          if (lastMessage?.role === 'user' && lastMessage.parts.some(p => p.functionResponse)) {
            lastMessage.parts.push(...parts);
          } else {
            contents.push({ role: 'user', parts });
          }
        }
      } else {
        // 非 tool_result 内容，按顺序累积
        const part = this.convertSingleUserPart(block);
        if (part) {
          if (Array.isArray(part)) {
            currentParts.push(...part);
          } else {
            currentParts.push(part);
          }
        }
      }
    }

    // flush 剩余的 parts
    flushCurrentParts();
  }

  /**
   * 转换单个 user 内容块为 Gemini part
   *
   * @param {object} block - Claude 内容块
   * @returns {object|Array|null} Gemini part 或 parts 数组
   */
  convertSingleUserPart(block) {
    switch (block.type) {
      case 'text':
        if (block.text?.trim()) {
          return { text: block.text };
        }
        return null;

      case 'image': {
        return this.convertImage(block);
      }

      case 'document': {
        const doc = this.convertDocument(block);
        if (doc) {
          return doc;
        } else if (block?.source?.type === 'content' && Array.isArray(block.source.content)) {
          // content 类型文档：递归处理嵌套内容
          const nestedParts = [];
          for (const nested of block.source.content) {
            const part = this.convertSingleUserPart(nested);
            if (part) {
              if (Array.isArray(part)) {
                nestedParts.push(...part);
              } else {
                nestedParts.push(part);
              }
            }
          }
          return nestedParts.length > 0 ? nestedParts : null;
        }
        return null;
      }

      default:
        return null;
    }
  }

  /**
   * 处理 assistant 消息，构建 Gemini contents
   *
   * @param {object} parsed - 解析后的内容结构
   * @param {Array} contents - Gemini contents 数组（会被修改）
   * @param {string} modelName - 模型名称
   * @param {string|Array} originalContent - 原始内容（用于保持顺序）
   */
  handleAssistantMessage(parsed, contents, modelName, originalContent = null) {
    // 判断是否支持 thoughtSignature：
    // 1. 模型名包含 gemini-3（直接调用 Gemini 3）
    // 2. 模型名包含 thinking（Claude thinking 模式，后端实际是 Gemini）
    // 3. 历史消息中有 signature（说��之前的响应来自支持 signature 的模型）
    const hasSignatureInHistory = parsed.thinkingParts.some(t => t.signature);
    const allowThoughtSignature = hasSignatureInHistory ||
      (typeof modelName === 'string' && (modelName.includes('gemini-3') || modelName.includes('thinking')));
    let parts = [];

    if (originalContent && Array.isArray(originalContent)) {
      parts = this.buildOrderedAssistantParts(originalContent, allowThoughtSignature);
    } else {
      // 处理 thinking 块（包括 signature-only）
      for (const thinkingBlock of parsed.thinkingParts) {
        const hasThinkingText = thinkingBlock.thinking !== undefined && thinkingBlock.thinking !== '';
        const hasSignature = Boolean(thinkingBlock.signature);
        if (!hasThinkingText && !hasSignature) continue;

        // 有签名且模型支持 thoughtSignature：使用 Gemini thought 格式
        if (hasSignature && allowThoughtSignature) {
          parts.push({
            thought: true,
            text: thinkingBlock.thinking ?? '',
            thoughtSignature: thinkingBlock.signature
          });
        } else if (hasThinkingText) {
          // 无签名或模型不支持：将 thinking 内容合并到文本中（用标签包裹以便区分）
          // 这样即使无签名，模型也能看到之前的思考内容
          parts.push({
            text: `<thinking>${thinkingBlock.thinking}</thinking>`
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
    }

    if (parts.length === 0) {
      return;
    }

    // 合并到上一条 model 消息
    const lastMessage = contents[contents.length - 1];
    const onlyToolCalls = parts.every(p => p.functionCall) && parts.length > 0;

    if (lastMessage?.role === 'model' && onlyToolCalls) {
      lastMessage.parts.push(...parts);
    } else {
      contents.push({ role: 'model', parts });
    }
  }

  /**
   * 按原始顺序构建 assistant 消息（保持所有内容类型的交错顺序）
   *
   * @param {Array} originalContent - Claude 原始内容数组
   * @param {boolean} allowThoughtSignature - 是否允许 thoughtSignature
   * @returns {Array} Gemini parts 数组
   */
  buildOrderedAssistantParts(originalContent, allowThoughtSignature) {
    const parts = [];

    for (const block of originalContent) {
      if (!block || typeof block !== 'object') continue;

      switch (block.type) {
        case 'thinking': {
          // 处理 signature-only thinking 块
          const hasThinkingText = block.thinking !== undefined && block.thinking !== '';
          const hasSignature = Boolean(block.signature);
          if (!hasThinkingText && !hasSignature) break;

          if (hasSignature && allowThoughtSignature) {
            parts.push({
              thought: true,
              text: block.thinking ?? '',
              thoughtSignature: block.signature
            });
          } else if (hasThinkingText) {
            parts.push({ text: `<thinking>${block.thinking}</thinking>` });
          }
          break;
        }

        case 'redacted_thinking':
          if (block.data) {
            const redactedText = '[redacted]';
            if (allowThoughtSignature) {
              parts.push({
                thought: true,
                text: redactedText,
                thoughtSignature: block.data
              });
            } else {
              parts.push({ text: `<thinking>${redactedText}</thinking>` });
            }
          }
          break;

        case 'text':
          if (block.text?.trim()) {
            const textThoughtSignature = allowThoughtSignature ? getTextThoughtSignature(block.text) : undefined;
            const textPart = { text: textThoughtSignature?.text ?? block.text };
            if (allowThoughtSignature && textThoughtSignature?.signature) {
              textPart.thoughtSignature = textThoughtSignature.signature;
            }
            parts.push(textPart);
          }
          break;

        case 'tool_use': {
          const id = block.id || generateToolUseId();
          const args = typeof block.input === 'string' ? safeJsonParse(block.input) : (block.input || {});
          const part = {
            functionCall: {
              id,
              name: block.name,
              args
            }
          };
          const thoughtSignature = getThoughtSignature(id);
          if (thoughtSignature) {
            part.thoughtSignature = thoughtSignature;
          }
          parts.push(part);
          break;
        }

        case 'image': {
          const imagePart = this.convertImage(block);
          if (imagePart) {
            parts.push(imagePart);
          }
          break;
        }

        case 'document': {
          const docPart = this.convertSingleUserPart(block);
          if (docPart) {
            if (Array.isArray(docPart)) {
              parts.push(...docPart);
            } else {
              parts.push(docPart);
            }
          }
          break;
        }

        default:
          break;
      }
    }

    return parts;
  }

  /**
   * 注入 Antigravity 系统前缀（仅 claude / gemini-3-pro 模型）
   *
   * @param {string|null} text - 原始系统指令
   * @param {string} modelName - 模型名称
   * @returns {string|null} 处理后的系统指令
   */
  maybeInjectAntigravityPrefix(text, modelName) {
    if (typeof modelName !== 'string') return text;
    const shouldInject = modelName.includes('claude') || modelName.includes('gemini-3-pro');
    if (!shouldInject) return text;
    const prefix = ANTIGRAVITY_SYSTEM_PREFIX;
    if (!prefix) return text;
    // 避免重复注入
    if (typeof text === 'string' && text.includes(prefix)) return text;
    return text ? `${prefix}\n\n${text}` : prefix;
  }

  /**
   * 提取系统指令
   *
   * @param {string|Array} system - Claude system 内容
   * @returns {string|null} 系统指令文本
   */
  extractSystemInstruction(system) {
    if (!system) return null;

    if (Array.isArray(system)) {
      return system
        .map(block => {
          if (typeof block === 'string') return block;
          if (block && typeof block === 'object' && 'text' in block) {
            return block.text || '';
          }
          return '';
        })
        .join('\n');
    }

    return system;
  }

  /**
   * 转换文本内容
   *
   * @param {string|object} content - 文本内容
   * @returns {object} Gemini 文本 part { text: string }
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { text: content };
    }
    if (content?.type === 'text') {
      return { text: content.text || '' };
    }
    return { text: '' };
  }

  /**
   * 转换图片内容 (Claude → Gemini)
   *
   * @param {object} block - Claude image 块
   * @returns {object|null} Gemini inlineData/fileData part
   */
  convertImage(block) {
    const source = block?.source;
    if (!source) return null;

    // base64 类型
    if (source.type === 'base64' && source.media_type && source.data) {
      return {
        inlineData: {
          mimeType: source.media_type,
          data: source.data
        }
      };
    }

    // URL 类型
    if (source.type === 'url' && source.url) {
      return {
        fileData: {
          fileUri: source.url,
          mimeType: source.media_type || 'image/jpeg'
        }
      };
    }

    return null;
  }

  /**
   * 转换文档内容 (Claude → Gemini)
   *
   * @param {object} block - Claude document 块
   * @returns {object|null} Gemini inlineData/fileData/text part
   */
  convertDocument(block) {
    const source = block?.source;
    if (!source) return null;

    // base64 类型
    if (source.type === 'base64' && source.media_type && source.data) {
      return {
        inlineData: {
          mimeType: source.media_type,
          data: source.data
        }
      };
    }

    // URL 类型
    if (source.type === 'url' && source.url) {
      return {
        fileData: {
          fileUri: source.url,
          mimeType: source.media_type || 'application/pdf'
        }
      };
    }

    // text 类型 - 纯文本文档
    if (source.type === 'text' && source.data) {
      return { text: source.data };
    }

    // content 类型在调用方处理（返回 null 触发递归）
    return null;
  }

  /**
   * 转换工具定义 (Claude → Gemini)
   *
   * @param {Array} tools - Claude 工具定义数组
   * @returns {Array} Gemini functionDeclarations 数组
   */
  convertTools(tools) {
    if (!tools || !Array.isArray(tools) || tools.length === 0) return [];

    return tools.map(tool => {
      if (!tool || typeof tool !== 'object') return null;

      const name = tool.name;
      const description = tool.description;
      const rawParameters = tool.input_schema || {};

      let parameters = {};
      if (rawParameters && typeof rawParameters === 'object') {
        try {
          parameters = typeof structuredClone === 'function'
            ? structuredClone(rawParameters)
            : JSON.parse(JSON.stringify(rawParameters));
        } catch {
          parameters = { ...rawParameters };
        }
      }
      const cleanedParameters = cleanJsonSchema(parameters);

      return {
        functionDeclarations: [{
          name,
          description,
          parameters: cleanedParameters
        }]
      };
    }).filter(Boolean);
  }

  /**
   * 转换工具选择配置 (Claude → Gemini)
   *
   * VALIDATED 模式说明：
   * CLIProxyAPI 对 Claude/Gemini-3 模型强制设置 VALIDATED 模式。
   * VALIDATED 模式要求 Gemini 更严格地验证工具调用参数，
   * 确保生成的参数符合 Schema 定义，提高工具调用的可靠性。
   *
   * 参考：CLIProxyAPI antigravity_executor.go:438
   *   template, _ = sjson.Set(template, "request.toolConfig.functionCallingConfig.mode", "VALIDATED")
   *
   * @param {object} toolChoice - Claude tool_choice 配置
   * @param {Array} tools - 工具列表
   * @param {string} modelName - 模型名称（用于判断是否启用 VALIDATED）
   * @returns {object} Gemini functionCallingConfig
   */
  convertToolConfig(toolChoice, tools, modelName = '') {
    // 判断是否应使用 VALIDATED 模式
    // 策略：对 Claude 和 Gemini-3 系列模型启用 VALIDATED
    const shouldValidate = modelName &&
      (modelName.includes('claude') || modelName.includes('gemini-3'));

    if (!toolChoice) {
      // VALIDATED 模式：强制验证工具调用参数
      // AUTO 模式：模型自动决定是否调用工具
      return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
    }

    switch (toolChoice.type) {
      case 'auto':
        return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
      case 'any':
        // ANY 模式：强制调用工具（从可用工具中选择）
        // 注意：VALIDATED 不改变 ANY 的语义，只增加参数验证
        return { mode: 'ANY' };
      case 'tool':
        // 指定特定工具
        return {
          mode: 'ANY',
          allowed_function_names: [toolChoice.name]
        };
      case 'none':
        // NONE 模式：禁止调用工具
        return { mode: 'NONE' };
      default:
        return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
    }
  }

  /**
   * 转换工具调用 (Claude tool_use → Gemini functionCall parts)
   *
   * @param {Array} toolCalls - Claude tool_use 块数组
   * @returns {Array} Gemini functionCall parts 数组
   */
  convertToolCalls(toolCalls) {
    if (!Array.isArray(toolCalls)) return [];

    return toolCalls
      .filter(call => call && typeof call === 'object')
      .map(call => {
        const id = call.id || generateToolUseId();
        const args = typeof call.input === 'string' ? safeJsonParse(call.input) : call.input;

        const part = {
          functionCall: {
            id,
            name: call.name,
            args
          }
        };

        const thoughtSignature = getThoughtSignature(id);
        if (thoughtSignature) {
          part.thoughtSignature = thoughtSignature;
        }

        return part;
      });
  }

  /**
   * 转换工具调用结果 (Claude tool_result → Gemini functionResponse parts)
   *
   * @param {object} toolResult - 解析后的 tool_result
   * @param {Array} contents - Gemini contents 数组（用于查找 functionName）
   * @returns {{ parts: Array }} Gemini parts 数组
   */
  convertToolResult(toolResult, contents) {
    // 1. 从历史消息中查找 functionName
    let functionName = '';
    for (let i = contents.length - 1; i >= 0; i--) {
      if (contents[i].role === 'model') {
        for (const part of contents[i].parts) {
          if (part?.functionCall?.id === toolResult.tool_use_id) {
            functionName = part.functionCall.name;
            break;
          }
        }
        if (functionName) break;
      }
    }

    // 2. 构建输出内容
    let outputContent = toolResult.content || '';
    if (toolResult.is_error) {
      outputContent = `Error: ${outputContent}`;
    }

    // 3. 构建 parts (Gemini API 要求 functionResponse 包含 id 匹配 functionCall.id)
    const parts = [{
      functionResponse: {
        id: toolResult.tool_use_id,
        name: functionName || 'unknown_tool',
        response: { output: outputContent }
      }
    }];

    if (toolResult.images?.length > 0) {
      parts.push(...toolResult.images);
    }
    if (toolResult.documents?.length > 0) {
      parts.push(...toolResult.documents);
    }

    return { parts };
  }

  /**
   * 构建生成配置
   *
   * @param {object} parameters - Claude 请求参数
   * @param {string} modelName - 模型名称
   * @returns {object} Gemini generationConfig
   */
  buildGenerationConfig(parameters, modelName) {
    const config = {};

    if (parameters.temperature !== undefined) {
      config.temperature = parameters.temperature;
    }
    if (parameters.max_tokens !== undefined) {
      config.maxOutputTokens = parameters.max_tokens;
    }
    if (parameters.top_p !== undefined) {
      config.topP = parameters.top_p;
    }
    if (parameters.top_k !== undefined) {
      config.topK = parameters.top_k;
    }
    if (parameters.stop_sequences !== undefined) {
      config.stopSequences = parameters.stop_sequences;
    }

    return config;
  }
}

export default ClaudeToGeminiRequestConverter;
