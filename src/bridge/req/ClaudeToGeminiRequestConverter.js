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
  ANTIGRAVITY_SYSTEM_PREFIX,
  unpackThinkingText,
  attachDefaultSafetySettings
} from '../common/index.js';
import { isThinkingModel, getThoughtSignature, getTextThoughtSignature, safeJsonParse } from '../../utils/utils.js';

// 注意：已移除 INVOKE_REGEX 和 TOOL_RESULT_REGEX
// 原因：全局正则替换会误删用户讨论 XML 或工具调用格式的正常文本内容
// Bridge 层不应擅自修改用户输入

// 与 CLIProxyAPI 保持一致：用于绕过后端签名验证的特殊值
const THOUGHT_SIGNATURE_SKIP = 'skip_thought_signature_validator';

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

    // 与 CLIProxyAPI 保持一致：附加默认安全设置
    // 参考: CLIProxyAPI antigravity_claude_request.go (使用 common.AttachDefaultSafetySettings)
    return attachDefaultSafetySettings(requestBody);
  }

  /**
   * 转换消息数组 (Claude → Gemini)
   *
   * 与 CLIProxyAPI (antigravity_claude_request.go) 保持一致：
   * 1. First pass: 构建 tool_use ID -> function name 映射表
   * 2. Second pass: 转换消息
   *
   * @param {Array} messages - Claude 消息数组
   * @param {string} modelName - 模型名称
   * @param {boolean} enableThinking - 是否启用思考模式
   * @returns {Array} Gemini contents 数组
   */
  convertMessages(messages, modelName, enableThinking = false) {
    if (!Array.isArray(messages)) return [];

    // First pass: 构建 tool_use ID -> function name 映射表
    // 参考: CLIProxyAPI antigravity_openai_request.go:148-166 的模式
    const toolUseID2Name = new Map();
    for (const message of messages) {
      if (message.role === 'assistant' && Array.isArray(message.content)) {
        for (const block of message.content) {
          if (block.type === 'tool_use' && block.id && block.name) {
            toolUseID2Name.set(block.id, block.name);
          }
        }
      }
    }

    // Second pass: 转换消息
    const contents = [];

    for (const message of messages) {
      const parsed = this.convertContent(message.content);

      if (message.role === 'user') {
        // 传递原始内容以保持顺序，同时传递工具映射表
        this.handleUserMessage(parsed, contents, enableThinking, message.content, toolUseID2Name);
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
          // 按 CLIProxyAPI 策略：只保留有有效签名的 thinking 块
          // 有效签名至少 50 字符长度
          // 无效签名的 thinking 块直接丢弃，不转换为文本
          // 解包 thinking 字段（可能是字符串、{text}、{thinking} 对象）
          if (block.signature && block.signature.length >= 50) {
            const thinkingText = unpackThinkingText(block.thinking);
            if (thinkingText) {
              result.thinkingParts.push({
                thinking: thinkingText,
                signature: block.signature
              });
            }
          }
          break;

        case 'redacted_thinking':
          // block.data 就是签名，需要检查有效性（至少 50 字符）
          if (block.data && block.data.length >= 50) {
            result.thinkingParts.push({
              thinking: '[redacted]',
              signature: block.data,
              redacted: true
            });
          }
          // 无有效签名的 redacted_thinking 块直接丢弃
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
   * @param {Map} toolUseID2Name - tool_use ID -> function name 映射表
   */
  handleUserMessage(parsed, contents, enableThinking, originalContent = null, toolUseID2Name = new Map()) {
    // 如果有原始内容数组，按原始顺序处理（保持所有内容类型的交错顺序）
    if (originalContent && Array.isArray(originalContent)) {
      this.buildOrderedUserMessage(originalContent, parsed.toolResults, contents, toolUseID2Name);
    } else if (typeof originalContent === 'string' && originalContent.trim()) {
      // 字符串内容
      contents.push({ role: 'user', parts: [{ text: originalContent }] });
    } else {
      // 兜底：先处理 tool_result，再处理其他内容（向后兼容）
      for (const toolResult of parsed.toolResults) {
        const { parts } = this.convertToolResult(toolResult, contents, toolUseID2Name);
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
   * @param {Map} toolUseID2Name - tool_use ID -> function name 映射表
   */
  buildOrderedUserMessage(originalContent, parsedToolResults, contents, toolUseID2Name = new Map()) {
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
          const { parts } = this.convertToolResult(parsedResult, contents, toolUseID2Name);
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
      // 消息级签名传播：记录当前消息中有效的 thinking 签名
      let currentMessageThinkingSignature = null;

      // 处理 thinking 块 - 按 CLIProxyAPI 策略只保留有效签名的块
      for (const thinkingBlock of parsed.thinkingParts) {
        // 有效签名检查已在 convertContent 中完成，这里只需确认有签名
        const hasValidSignature = thinkingBlock.signature && thinkingBlock.signature.length >= 50;
        if (!hasValidSignature) continue;

        // 记录有效签名，用于传递给后续的 tool_use
        currentMessageThinkingSignature = thinkingBlock.signature;

        // 有有效签名且模型支持 thoughtSignature：使用 Gemini thought 格式
        if (allowThoughtSignature) {
          // thinkingBlock.thinking 已经在 convertContent 中通过 unpackThinkingText 解包
          parts.push({
            thought: true,
            text: thinkingBlock.thinking || '',
            thoughtSignature: thinkingBlock.signature
          });
        }
        // 不支持 thoughtSignature 的模型，丢弃 thinking 块
      }

      // 处理文本内容
      if (parsed.textParts.length > 0) {
        const contentText = parsed.textParts.join('\n');
        const textThoughtSignature = allowThoughtSignature ? getTextThoughtSignature(contentText) : undefined;

        const textPart = { text: textThoughtSignature?.text ?? contentText };
        if (allowThoughtSignature && textThoughtSignature?.signature) {
          textPart.thoughtSignature = textThoughtSignature.signature;
          // 文本签名也可以传播给后续 tool_use
          if (!currentMessageThinkingSignature) {
            currentMessageThinkingSignature = textThoughtSignature.signature;
          }
        }
        parts.push(textPart);
      }

      // 处理 tool_use
      for (const toolCall of parsed.toolCalls) {
        const cachedSignature = getThoughtSignature(toolCall.id);
        const part = {
          functionCall: {
            id: toolCall.id,
            name: toolCall.name,
            args: typeof toolCall.input === 'string' ? safeJsonParse(toolCall.input) : toolCall.input
          }
        };
        // 签名优先级（与 CLIProxyAPI 保持一致）：
        // 1. 缓存中的签名
        // 2. 当前消息中的 thinking 签名（消息级传播）
        // 3. SKIP 绕过验证
        if (cachedSignature) {
          part.thoughtSignature = cachedSignature;
        } else if (currentMessageThinkingSignature) {
          part.thoughtSignature = currentMessageThinkingSignature;
        } else {
          // 与 CLIProxyAPI 保持一致：缓存未命中时使用 SKIP 绕过验证
          part.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
        }
        parts.push(part);
      }
    }

    // 重排序 parts：确保 thinking blocks 在最前面（非 originalContent 路径）
    // 参考 CLIProxyAPI antigravity_claude_request.go:276-304
    if (parts.length > 1) {
      const thinkingParts = [];
      const otherParts = [];
      for (const part of parts) {
        if (part && part.thought === true) {
          thinkingParts.push(part);
        } else {
          otherParts.push(part);
        }
      }
      if (thinkingParts.length > 0 && parts[0]?.thought !== true) {
        parts = [...thinkingParts, ...otherParts];
      }
      // NOTE: 不注入占位 thinking 块！
      // 参考 CLIProxyAPI antigravity_claude_request.go:182-183
      // "Antigravity API validates signatures, so dummy values are rejected."
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
   * 关键优化（与 CLIProxyAPI 保持一致）：
   * - 实现消息级签名传播 (currentMessageThinkingSignature)
   * - 当一个 assistant 消息中有有效的 thinking 块时，其签名会传递给同一消息中的后续 tool_use
   * - 参考：CLIProxyAPI antigravity_claude_request.go:150-152, 211-216
   *
   * @param {Array} originalContent - Claude 原始内容数组
   * @param {boolean} allowThoughtSignature - 是否允许 thoughtSignature
   * @returns {Array} Gemini parts 数组
   */
  buildOrderedAssistantParts(originalContent, allowThoughtSignature) {
    const parts = [];
    // 消息级签名传播：记录当前消息中有效的 thinking 签名
    // 用于传递给同一消息中后续的 tool_use（与 CLIProxyAPI 的 currentMessageThinkingSignature 一致）
    let currentMessageThinkingSignature = null;

    for (const block of originalContent) {
      if (!block || typeof block !== 'object') continue;

      switch (block.type) {
        case 'thinking': {
          // 按 CLIProxyAPI 策略：只保留有有效签名（>=50字符）的 thinking 块
          // 无效签名的 thinking 块直接丢弃，不转换为 <thinking> 标签文本
          const hasValidSignature = block.signature && block.signature.length >= 50;
          if (!hasValidSignature) break;

          // 解包 thinking 字段（可能是字符串、{text}、{thinking} 对象）
          const thinkingText = unpackThinkingText(block.thinking);
          // 与 CLIProxyAPI antigravity_claude_request.go:167-169 一致：
          // 跳过空 thinking 文本的块，避免发送 { thought: true, text: "" }
          if (!thinkingText) break;

          // 记录有效签名，用于传递给后续的 tool_use（消息级签名传播）
          currentMessageThinkingSignature = block.signature;

          if (allowThoughtSignature) {
            parts.push({
              thought: true,
              text: thinkingText,
              thoughtSignature: block.signature
            });
          }
          // 不支持 thoughtSignature 的情况下，有效签名的 thinking 块也丢弃（无法正确透传）
          break;
        }

        case 'redacted_thinking':
          // block.data 就是签名，需要检查有效性（至少 50 字符）
          if (block.data && block.data.length >= 50) {
            // 记录有效签名，用于传递给后续的 tool_use（消息级签名传播）
            currentMessageThinkingSignature = block.data;

            if (allowThoughtSignature) {
              parts.push({
                thought: true,
                text: '[redacted]',
                thoughtSignature: block.data
              });
            }
          }
          // 无效签名或不支持 thoughtSignature 时直接丢弃
          break;

        case 'text':
          if (block.text?.trim()) {
            const textThoughtSignature = allowThoughtSignature ? getTextThoughtSignature(block.text) : undefined;
            const textPart = { text: textThoughtSignature?.text ?? block.text };
            if (allowThoughtSignature && textThoughtSignature?.signature) {
              textPart.thoughtSignature = textThoughtSignature.signature;
              // 文本签名也可以传播给后续 tool_use
              if (!currentMessageThinkingSignature) {
                currentMessageThinkingSignature = textThoughtSignature.signature;
              }
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
          // 签名优先级（与 CLIProxyAPI 保持一致）：
          // 1. 缓存中的签名（之前响应记录的）
          // 2. 当前消息中的 thinking 签名（消息级传播）
          // 3. SKIP 绕过验证
          const cachedSignature = getThoughtSignature(id);
          if (cachedSignature) {
            part.thoughtSignature = cachedSignature;
          } else if (currentMessageThinkingSignature) {
            // 消息级签名传播：使用当前消息中 thinking 块的签名
            part.thoughtSignature = currentMessageThinkingSignature;
          } else {
            // 与 CLIProxyAPI 保持一致：缓存未命中时使用 SKIP 绕过验证
            part.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
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

    // 重排序 parts：确保 thinking blocks 在最前面
    // 参考 CLIProxyAPI antigravity_claude_request.go:276-304
    // 解决 "Expected thinking or redacted_thinking, but found tool_use" 错误
    const thinkingParts = [];
    const otherParts = [];

    for (const part of parts) {
      if (part && part.thought === true) {
        thinkingParts.push(part);
      } else {
        otherParts.push(part);
      }
    }

    // 如果有 thinking parts 且不在最前面，重新排序
    if (thinkingParts.length > 0) {
      const firstPartIsThinking = parts[0]?.thought === true;
      if (!firstPartIsThinking || thinkingParts.length > 1) {
        return [...thinkingParts, ...otherParts];
      }
    }

    // NOTE: 不注入占位 thinking 块！
    // 参考 CLIProxyAPI antigravity_claude_request.go:182-183
    // "Antigravity API validates signatures, so dummy values are rejected."

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
        } else {
          // 与 CLIProxyAPI 保持一致：缓存未命中时使用 SKIP 绕过验证
          part.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
        }

        return part;
      });
  }

  /**
   * 转换工具调用结果 (Claude tool_result → Gemini functionResponse parts)
   *
   * 与 CLIProxyAPI 保持一致：优先使用 first-pass 构建的 toolUseID2Name 映射表查找函数名
   * 参考: CLIProxyAPI antigravity_openai_request.go:307-314
   *
   * @param {object} toolResult - 解析后的 tool_result
   * @param {Array} contents - Gemini contents 数组（备用查找 functionName）
   * @param {Map} toolUseID2Name - tool_use ID -> function name 映射表
   * @returns {{ parts: Array }} Gemini parts 数组
   */
  convertToolResult(toolResult, contents, toolUseID2Name = new Map()) {
    // 优先使用 first-pass 构建的映射表（与 CLIProxyAPI 一致）
    let functionName = toolUseID2Name.get(toolResult.tool_use_id) || '';

    // 备用：从历史消息中查找 functionName
    if (!functionName) {
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
