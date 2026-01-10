/**
 * OpenAI → Gemini 请求转换器
 *
 * 输入: OpenAI Chat Completions API 请求格式
 * 输出: Gemini API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import {
  cleanJsonSchema,
  resolveThinkingBudget,
  normalizeThinkingBudget,
  shouldUseThinkingLevel,
  mergeConsecutiveRoles,
  ANTIGRAVITY_SYSTEM_PREFIX,
  DATA_URL_REGEX,
  DOCUMENT_MIME_TYPES,
  AUDIO_FORMAT_MIME,
  EXTENSION_MIME_MAP,
  attachDefaultSafetySettings
} from '../common/index.js';
import {
  safeJsonParse,
  safeJsonStringify,
  getTextThoughtSignature
} from '../../utils/utils.js';

const THOUGHT_SIGNATURE_SKIP = 'skip_thought_signature_validator';

export class OpenAIToGeminiRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体 (OpenAI → Gemini)
   *
   * @param {object} body - OpenAI Chat Completions API 请求体
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

    const { messages, tools, tool_choice, ...parameters } = body;
    const modelName = context.model || body.model || 'gemini-pro';

    const enableThinking = this.resolveThinkingEnabled(parameters, modelName);
    const contents = this.convertMessages(messages, modelName, enableThinking);
    const geminiTools = this.convertTools(tools);
    // 传入 modelName 以支持 VALIDATED 模式判断
    const toolConfig = this.convertToolConfig(tool_choice, tools, modelName);

    // 提取系统指令
    const systemInstruction = this.extractSystemInstruction(messages);

    // [Antigravity] 注入身份前缀 (模拟 CLIProxyAPI 行为)
    const finalSystemInstruction = this.maybeInjectAntigravityPrefix(systemInstruction, modelName);

    // 确保消息角色交替（Gemini 强制要求 User/Model 交替）
    const mergedContents = mergeConsecutiveRoles(contents);
    // 与 CLIProxyAPI 保持一致：无条件为 functionCall/inlineData 添加 thoughtSignature
    this.ensureThinkingPrefixForToolCalls(mergedContents);
    if (mergedContents.length === 0) {
      mergedContents.push({ role: 'user', parts: [{ text: '' }] });
    } else if (mergedContents[0].role !== 'user') {
      mergedContents.unshift({ role: 'user', parts: [{ text: '' }] });
    }

    // 构建请求体
    const requestBody = {
      contents: mergedContents,
      generationConfig: this.buildGenerationConfig(parameters, modelName)
    };

    if (finalSystemInstruction) {
      requestBody.systemInstruction = { parts: [{ text: finalSystemInstruction }] };
    }

    if (geminiTools.length > 0) {
      requestBody.tools = geminiTools;
      requestBody.toolConfig = { functionCallingConfig: toolConfig };
    }

    // 与 CLIProxyAPI 保持一致：附加默认安全设置
    // 参考: CLIProxyAPI antigravity_openai_request.go:409
    return attachDefaultSafetySettings(requestBody);
  }

  /**
   * 转换消息数组 (OpenAI → Gemini)
   *
   * 与 CLIProxyAPI (antigravity_openai_request.go) 保持一致：
   * 1. First pass: 构建 tool_call ID -> function name 映射表
   * 2. Second pass: 转换消息
   *
   * @param {Array} messages - OpenAI 消息数组
   * @param {string} modelName - 模型名称
   * @param {boolean} enableThinking - 是否启用思考模式
   * @returns {Array} Gemini contents 数组
   */
  convertMessages(messages, modelName, enableThinking = false) {
    if (!Array.isArray(messages)) return [];

    // First pass: 构建 tool_call ID -> function name 映射表
    // 参考: CLIProxyAPI antigravity_openai_request.go:148-166
    const tcID2Name = new Map();
    for (const message of messages) {
      if (message.role === 'assistant' && Array.isArray(message.tool_calls)) {
        for (const tc of message.tool_calls) {
          if (tc.type === 'function' && tc.id && tc.function?.name) {
            tcID2Name.set(tc.id, tc.function.name);
          }
        }
      }
    }

    // Second pass: 转换消息
    const contents = [];

    for (const message of messages) {
      if (message.role === 'system' || message.role === 'developer') {
        // system/developer 消息单独处理为 systemInstruction，这里跳过
        continue;
      }

      if (message.role === 'user') {
        const parts = this.convertContent(message.content);
        contents.push({ role: 'user', parts });
      } else if (message.role === 'assistant') {
        const parts = this.buildAssistantParts(message, modelName, enableThinking);
        contents.push({ role: 'model', parts });
      } else if (message.role === 'tool') {
        // 使用预构建的映射表查找函数名
        const parts = this.buildToolResultParts(message, contents, tcID2Name);
        contents.push({ role: 'user', parts });
      }
    }

    return contents;
  }

  /**
   * 转换内容块为 Gemini parts (OpenAI → Gemini)
   *
   * 注意：始终返回 Gemini parts 数组格式
   *
   * @param {string|Array} content - OpenAI 内容（字符串或内容块数组）
   * @returns {Array} Gemini parts 数组
   */
  convertContent(content) {
    if (!content) return [{ text: '' }];
    if (typeof content === 'string') return [{ text: content }];
    if (!Array.isArray(content)) return [{ text: '' }];

    const parts = [];

    for (const item of content) {
      if (!item || typeof item !== 'object') continue;

      switch (item.type) {
        case 'text':
          if (item.text) {
            parts.push({ text: item.text });
          }
          break;

        case 'image_url':
          const imgPart = this.convertImage(item);
          if (imgPart) parts.push(imgPart);
          break;

        case 'input_audio':
          const audioPart = this.convertAudio(item);
          if (audioPart) parts.push(audioPart);
          break;

        case 'file':
          const filePart = this.convertDocument(item);
          if (filePart) parts.push(filePart);
          break;

        case 'input_file':
          // OpenAI Responses API input_file 格式
          const inputFilePart = this.convertInputFile(item);
          if (inputFilePart) parts.push(inputFilePart);
          break;
      }
    }

    return parts.length > 0 ? parts : [{ text: '' }];
  }

  /**
   * 构建助手消息的 parts (OpenAI → Gemini)
   *
   * 与 CLIProxyAPI antigravity_openai_request.go:248-276 完全一致：
   * - 字符串 content：正常写入
   * - 数组 content：只处理 image_url，忽略 text/thinking 等其他类型
   * - tool_calls：正常处理，添加 thoughtSignature
   *
   * @param {object} message - OpenAI assistant 消息
   * @param {string} modelName - 模型名称
   * @param {boolean} enableThinking - 是否启用思考模式
   * @returns {Array} Gemini parts 数组
   */
  buildAssistantParts(message, modelName = '', enableThinking = false) {
    const parts = [];

    // 处理 reasoning_content（OpenAI o1/o3 格式的思考内容）
    // 必须放在最前面，对应 Claude 的 thinking block 要求
    // 注意：这是对 CLIProxyAPI 的扩展，用于支持多轮 thinking 对话
    if (message.reasoning_content) {
      const thoughtPart = {
        text: message.reasoning_content,
        thought: true  // Gemini 格式的思考标记
      };
      // 签名优先级：1. 透传签名 2. 缓存签名 3. SKIP
      if (message.reasoning_signature) {
        thoughtPart.thoughtSignature = message.reasoning_signature;
      } else {
        // 尝试从缓存获取签名（按文本内容查找）
        const cached = getTextThoughtSignature(message.reasoning_content);
        if (cached?.signature) {
          thoughtPart.thoughtSignature = cached.signature;
        } else {
          // 没有签名时使用跳过标记
          thoughtPart.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
        }
      }
      parts.push(thoughtPart);
    } else if (enableThinking) {
      // 当启用 thinking 但没有 reasoning_content 时，尝试从 content 数组中查找 thinking 块
      // 这是为了确保 Antigravity 后端转换为 Claude 格式时能正确生成 thinking block
      // Claude API 要求：启用 thinking 后，所有 assistant 消息必须以 thinking 开头
      let thinkingText = null;
      let thinkingSignature = null;

      // 尝试从 content 数组中查找 thinking 类型的块
      if (Array.isArray(message.content)) {
        for (const item of message.content) {
          if (item?.type === 'thinking' && item.thinking) {
            thinkingText = item.thinking;
            thinkingSignature = item.signature;
            break;
          } else if (item?.type === 'redacted_thinking') {
            // redacted_thinking: 使用 [redacted] 作为文本，签名从 data 字段获取
            thinkingText = '[redacted]';
            thinkingSignature = item.data || item.signature;
            break;
          }
        }
      }

      // 如果找到了 thinking 内容，使用它；否则使用 [redacted] 占位符
      const text = thinkingText || '[redacted]';
      const thoughtPart = {
        text,
        thought: true
      };

      // 签名优先级：1. 透传签名 2. 缓存签名 3. SKIP
      if (thinkingSignature) {
        thoughtPart.thoughtSignature = thinkingSignature;
      } else if (thinkingText) {
        const cached = getTextThoughtSignature(thinkingText);
        if (cached?.signature) {
          thoughtPart.thoughtSignature = cached.signature;
        } else {
          thoughtPart.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
        }
      } else {
        thoughtPart.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
      }

      parts.push(thoughtPart);
    }

    // 处理 content（与 CLIProxyAPI 完全一致）
    if (message.content) {
      if (typeof message.content === 'string' && message.content !== '') {
        // 字符串 content：正常写入（CLIProxyAPI 第 251-253 行）
        parts.push({ text: message.content });
      } else if (Array.isArray(message.content)) {
        // 数组 content：只处理 image_url，忽略 text/thinking（CLIProxyAPI 第 254-276 行）
        // 注意：CLIProxyAPI 对 text 类型只做 p++，不实际写入内容
        for (const item of message.content) {
          if (!item) continue;

          if (item.type === 'image_url') {
            // 处理图片（与 CLIProxyAPI 第 260-273 行一致）
            const imgPart = this.convertImage(item);
            if (imgPart) {
              parts.push(imgPart);
            }
          }
          // text, thinking 等其他类型：与 CLIProxyAPI 一致，完全忽略
        }
      }
    }

    // 处理工具调用（与 CLIProxyAPI 第 278-301 行一致）
    const hasToolCalls = Array.isArray(message.tool_calls) && message.tool_calls.length > 0;
    if (hasToolCalls) {
      for (const toolCall of message.tool_calls) {
        const args = safeJsonParse(toolCall.function?.arguments);

        const part = {
          functionCall: {
            id: toolCall.id,
            name: toolCall.function?.name,
            args
          },
          // CLIProxyAPI 第 296 行：无条件为所有 functionCall 添加 thoughtSignature
          thoughtSignature: THOUGHT_SIGNATURE_SKIP
        };

        parts.push(part);
      }
    }

    return parts;
  }

  /**
   * 构建工具结果的 parts (OpenAI → Gemini)
   *
   * 与 CLIProxyAPI 保持一致：使用 first-pass 构建的 tcID2Name 映射表查找函数名
   * 参考: CLIProxyAPI antigravity_openai_request.go:307-314
   *
   * @param {object} message - OpenAI tool 消息
   * @param {Array} contents - 已转换的 Gemini contents（备用查找）
   * @param {Map} tcID2Name - tool_call ID -> function name 映射表
   * @returns {Array} Gemini functionResponse parts 数组
   */
  buildToolResultParts(message, contents, tcID2Name = new Map()) {
    // 优先使用 first-pass 构建的映射表（与 CLIProxyAPI 一致）
    let functionName = tcID2Name.get(message.tool_call_id) || '';

    // 备用：从已转换的 contents 中查找
    if (!functionName) {
      for (let i = contents.length - 1; i >= 0; i--) {
        if (contents[i].role === 'model') {
          const parts = contents[i].parts;
          for (const part of parts) {
            if (part?.functionCall?.id === message.tool_call_id) {
              functionName = part.functionCall.name;
              break;
            }
          }
          if (functionName) break;
        }
      }
    }

    // 使用 convertContent 处理工具结果内容
    const convertedParts = this.convertContent(message.content);

    // 分离文本和媒体
    const textParts = convertedParts.filter(p => p.text !== undefined);
    const mediaParts = convertedParts.filter(p => p.inlineData || p.fileData);
    const outputText = textParts.map(p => p.text).join('\n');

    const parts = [];
    parts.push({
      functionResponse: {
        id: message.tool_call_id,
        name: functionName || message.name || 'unknown_tool',
        response: {
          output: outputText || ''
        }
      }
    });

    // 添加媒体内容
    if (mediaParts.length > 0) {
      parts.push(...mediaParts);
    }

    return parts;
  }

  /**
   * 判断是否启用 thinking 模式
   *
   * 与 CLIProxyAPI 保持一致，检查以下条件：
   * 1. 顶级 reasoning_effort 字段（OpenAI 官方格式）
   * 2. 嵌套 reasoning.effort 字段（Claude Code CLI 格式）
   * 3. 模型名称包含 -thinking 后缀
   *
   * CLIProxyAPI 通过模型注册表将 *-thinking 模型标记为需要 thinking，
   * 然后通过 ApplyDefaultThinkingIfNeededCLI 自动注入配置。
   * 本项目通过模型名检测来实现相同效果。
   */
  resolveThinkingEnabled(parameters = {}, modelName = '') {
    // 1. 检查顶级 reasoning_effort 字段 (OpenAI 官方格式)
    if (typeof parameters?.reasoning_effort === 'string') {
      return true;
    }

    // 2. 检查嵌套 reasoning.effort 字段 (Claude Code CLI 格式)
    if (parameters?.reasoning && typeof parameters.reasoning.effort === 'string') {
      return true;
    }

    // 3. 检查 thinking 对象 (Claude 原生格式，直接支持)
    // 这样可以避免先经过 ClaudeToOpenAI 再转换的多余链路
    if (parameters?.thinking?.type === 'enabled' && parameters.thinking.budget_tokens) {
      return true;
    }

    // 4. 检查模型名称是否包含 -thinking 后缀
    // 这对应 CLIProxyAPI 的 ApplyDefaultThinkingIfNeededCLI 逻辑
    if (typeof modelName === 'string' && modelName.includes('-thinking')) {
      return true;
    }

    return false;
  }

  /**
   * 获取 reasoning effort 值
   *
   * 按优先级返回：reasoning_effort > reasoning.effort > thinking.budget_tokens > 默认值
   *
   * @param {object} parameters - 请求参数
   * @param {string} modelName - 模型名称（用于检测 -thinking 后缀）
   * @returns {string|null} effort 值
   */
  getReasoningEffort(parameters = {}, modelName = '') {
    // 1. 顶级 reasoning_effort 优先
    if (typeof parameters?.reasoning_effort === 'string') {
      return parameters.reasoning_effort;
    }

    // 2. 嵌套 reasoning.effort
    if (parameters?.reasoning && typeof parameters.reasoning.effort === 'string') {
      return parameters.reasoning.effort;
    }

    // 3. thinking 对象 (Claude 原生格式) - 将 budget_tokens 转换为 effort
    if (parameters?.thinking?.type === 'enabled' && parameters.thinking.budget_tokens) {
      const budget = parameters.thinking.budget_tokens;
      // 使用与 mappingUtils.resolveReasoningEffort 相同的阈值逻辑
      if (budget < 7500) return 'low';
      if (budget < 15000) return 'medium';
      return 'high';
    }

    // 4. 模型名包含 -thinking 时，返回默认值
    if (typeof modelName === 'string' && modelName.includes('-thinking')) {
      return 'high'; // 默认使用 high effort
    }

    return null;
  }

  /**
   * 为工具调用添加 thoughtSignature（不注入假 thinking 块）
   *
   * 参考 CLIProxyAPI antigravity_claude_request.go:184-216:
   * "Do NOT inject dummy thinking blocks here. Antigravity API validates signatures,
   * so dummy values are rejected."
   *
   * 与 CLIProxyAPI 保持一致：无条件为 functionCall 和 inlineData 添加 thoughtSignature，
   * 不注入假的 thinking 块（避免 Gemini→Claude 转换时格式错误）
   */
  ensureThinkingPrefixForToolCalls(contents) {
    if (!Array.isArray(contents)) return;

    for (const content of contents) {
      if (!content || content.role !== 'model' || !Array.isArray(content.parts)) {
        continue;
      }

      // 无条件为 functionCall 和 inlineData 添加 thoughtSignature（与 CLIProxyAPI 一致）
      for (const part of content.parts) {
        if (part && (part.functionCall || part.inlineData) && !part.thoughtSignature) {
          part.thoughtSignature = THOUGHT_SIGNATURE_SKIP;
        }
      }
    }
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
   * 提取系统指令 (OpenAI → Gemini)
   *
   * 注意：包括 system 和 developer 角色的消息
   *
   * @param {Array} messages - OpenAI 消息数组
   * @returns {string|null} 系统指令文本
   */
  extractSystemInstruction(messages) {
    if (!Array.isArray(messages)) return null;

    const systemMessages = messages.filter(m => m.role === 'system' || m.role === 'developer');
    if (systemMessages.length === 0) return null;

    return systemMessages
      .map(m => {
        if (typeof m.content === 'string') {
          return m.content;
        }
        // 处理数组格式内容
        if (Array.isArray(m.content)) {
          return m.content
            .filter(p => p.type === 'text')
            .map(p => p.text || '')
            .join('\n');
        }
        return '';
      })
      .filter(Boolean)
      .join('\n\n');
  }

  /**
   * 转换文本内容 (OpenAI → Gemini)
   *
   * @param {string|object} content - OpenAI 文本内容
   * @returns {object} Gemini 文本 part { text: string }
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { text: content };
    }
    return { text: content?.text || '' };
  }

  /**
   * 转换图片内容 (OpenAI → Gemini)
   *
   * 注意：支持 data URL 与 http(s) URL，URL 会尝试推断 MIME 类型
   *
   * @param {object} content - OpenAI image_url 块
   * @returns {object|null} Gemini inlineData/fileData part
   */
  convertImage(content) {
    // OpenAI: { type: 'image_url', image_url: { url: '...' } }
    const url = content?.image_url?.url || content?.url || '';

    const match = url.match(DATA_URL_REGEX);
    if (match) {
      // 与 CLIProxyAPI 保持一致：inlineData 无条件添加 thoughtSignature
      return {
        inlineData: {
          mimeType: match[1],
          data: match[2]
        },
        thoughtSignature: THOUGHT_SIGNATURE_SKIP
      };
    }

    if (url.startsWith('http://') || url.startsWith('https://')) {
      // 尝试从 URL 推断 MIME 类型
      const mimeType = this.guessMimeTypeFromUrl(url) || 'image/jpeg';
      return {
        fileData: {
          fileUri: url,
          mimeType
        }
      };
    }

    return null;
  }

  /**
   * 从 URL 推断图片 MIME 类型 (OpenAI → Gemini)
   *
   * @param {string} url - 图片 URL
   * @returns {string|null} 推断得到的 MIME 类型
   */
  guessMimeTypeFromUrl(url) {
    if (!url) return null;
    try {
      const pathname = new URL(url).pathname.toLowerCase();
      const ext = pathname.split('.').pop();
      return EXTENSION_MIME_MAP[ext] || null;
    } catch {
      // URL 解析失败，返回 null
    }
    return null;
  }

  /**
   * 转换文档内容 (OpenAI → Gemini)
   *
   * 注意：Data URL 输出 inlineData，URL 输出 fileData
   *
   * @param {object} content - OpenAI file 块
   * @returns {object|null} Gemini inlineData/fileData part
   */
  convertDocument(content) {
    const file = content?.file;
    if (!file) return null;

    const fileData = file.file_data || '';
    const filename = file.filename || '';

    // base64 Data URL
    const match = fileData.match(DATA_URL_REGEX);
    if (match) {
      const mimeType = match[1];
      // 区分图片和文档
      if (mimeType.startsWith('image/')) {
        return {
          inlineData: { mimeType, data: match[2] }
        };
      } else if (DOCUMENT_MIME_TYPES.includes(mimeType)) {
        return {
          inlineData: { mimeType, data: match[2] }
        };
      }
    }

    // URL 类型 - 根据文件名推断 MIME
    if (fileData.startsWith('http://') || fileData.startsWith('https://')) {
      const mimeType = this.guessMimeTypeFromFilename(filename) || 'application/pdf';
      return {
        fileData: { fileUri: fileData, mimeType }
      };
    }

    // file_id 引用 - Gemini 不支持，返回文本占位符
    if (file.file_id) {
      return { text: `[File reference not supported: ${file.file_id}]` };
    }

    return null;
  }

  /**
   * 根据文件名推断 MIME 类型 (OpenAI → Gemini)
   *
   * @param {string} filename - 文件名
   * @returns {string|null} 推断得到的 MIME 类型
   */
  guessMimeTypeFromFilename(filename) {
    if (!filename) return null;
    const ext = filename.split('.').pop()?.toLowerCase();
    return EXTENSION_MIME_MAP[ext] || null;
  }

  /**
   * 转换音频内容 (OpenAI → Gemini)
   *
   * 注意：使用格式映射表推断 MIME 类型
   *
   * @param {object} content - OpenAI input_audio 块
   * @returns {object|null} Gemini inlineData part
   */
  convertAudio(content) {
    const audio = content?.input_audio;
    if (!audio?.data) return null;

    const mimeType = AUDIO_FORMAT_MIME[audio.format] || 'audio/wav';
    return {
      inlineData: { mimeType, data: audio.data }
    };
  }

  /**
   * 转换 input_file 内容 (OpenAI → Gemini)
   *
   * OpenAI Responses API 的 input_file 格式。
   *
   * @param {object} item - OpenAI input_file 块
   * @returns {object|null} Gemini inlineData/fileData/text part
   */
  convertInputFile(item) {
    if (!item) return null;

    // file_data (base64 Data URL)
    if (item.file_data) {
      const match = item.file_data.match(DATA_URL_REGEX);
      if (match) {
        return {
          inlineData: {
            mimeType: match[1],
            data: match[2]
          }
        };
      }

      // URL 格式
      if (item.file_data.startsWith('http://') || item.file_data.startsWith('https://')) {
        const mimeType = this.guessMimeTypeFromFilename(item.filename) || 'application/octet-stream';
        return {
          fileData: {
            fileUri: item.file_data,
            mimeType
          }
        };
      }
    }

    // file_id 引用 - Gemini 不支持
    if (item.file_id) {
      return { text: `[File reference not supported: ${item.file_id}]` };
    }

    return null;
  }

  /**
   * 转换工具调用 (OpenAI → Gemini)
   *
   * 注意：会尝试附加 thoughtSignature
   *
   * @param {Array} toolCalls - OpenAI tool_calls 数组
   * @returns {Array} Gemini functionCall parts 数组
   */
  convertToolCalls(toolCalls) {
    if (!Array.isArray(toolCalls)) return [];

    const parts = [];

    for (const toolCall of toolCalls) {
      if (!toolCall) continue;

      const args = safeJsonParse(toolCall.function?.arguments);
      const part = {
        functionCall: {
          id: toolCall.id,
          name: toolCall.function?.name,
          args
        }
      };

      const signature = getThoughtSignature(toolCall.id);
      if (signature) {
        part.thoughtSignature = signature;
      }

      parts.push(part);
    }

    return parts;
  }

  /**
   * 转换工具定义 (OpenAI → Gemini)
   *
   * 注意：兼容 OpenAI function 与 Claude 风格输入，且会清理 JSON Schema
   *
   * @param {Array} tools - OpenAI 工具定义数组
   * @returns {Array} Gemini functionDeclarations 数组
   */
  convertTools(tools) {
    if (!tools || !Array.isArray(tools) || tools.length === 0) return [];

    return tools.map(tool => {
      if (!tool || typeof tool !== 'object') return null;

      const isOpenAIFormat = (tool.type === 'function' || tool.function) && typeof tool.function === 'object';

      const name = isOpenAIFormat ? tool.function.name : tool.name;
      const description = isOpenAIFormat ? tool.function.description : tool.description;
      const rawParameters = isOpenAIFormat
        ? (tool.function?.parameters || {})
        : (tool.input_schema || {});

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
   * 转换工具选择配置 (OpenAI → Gemini)
   *
   * VALIDATED 模式说明：
   * CLIProxyAPI 对 Claude/Gemini-3 模型强制设置 VALIDATED 模式。
   * VALIDATED 模式要求 Gemini 更严格地验证工具调用参数，
   * 确保生成的参数符合 Schema 定义，提高工具调用的可靠性。
   *
   * 参考：CLIProxyAPI antigravity_executor.go:438
   *
   * 注意：映射 auto/none/required 到 Gemini functionCallingConfig
   *
   * @param {string|object} toolChoice - OpenAI tool_choice 配置
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
      return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
    }

    if (typeof toolChoice === 'string') {
      switch (toolChoice) {
        case 'auto':
          return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
        case 'none':
          return { mode: 'NONE' };
        case 'required':
          // required 模式：强制调用工具
          return { mode: 'ANY' };
        default:
          return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
      }
    }

    if (toolChoice.type === 'function' && toolChoice.function?.name) {
      return {
        mode: 'ANY',
        allowed_function_names: [toolChoice.function.name]
      };
    }

    return { mode: shouldValidate ? 'VALIDATED' : 'AUTO' };
  }

  /**
   * 转换工具调用结果 (OpenAI → Gemini)
   *
   * @param {object} result - OpenAI tool 消息
   * @returns {object} Gemini functionResponse part
   */
  convertToolResult(result) {
    return {
      functionResponse: {
        id: result.tool_call_id,
        name: result.name || 'unknown_tool',
        response: {
          output: typeof result.content === 'string' ? result.content : safeJsonStringify(result.content)
        }
      }
    };
  }

  /**
   * 构建生成配置 (OpenAI → Gemini)
   *
   * 注意：支持 reasoning effort 映射到 thinkingConfig，
   * 并应用 budget 规范化（min/max 限制）
   *
   * 参考：CLIProxyAPI normalizeAntigravityThinking
   *
   * @param {object} parameters - OpenAI 请求参数
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
    } else if (parameters.max_completion_tokens !== undefined) {
      // OpenAI o1/o3 模型使用 max_completion_tokens
      config.maxOutputTokens = parameters.max_completion_tokens;
    }
    if (parameters.top_p !== undefined) {
      config.topP = parameters.top_p;
    }
    if (parameters.top_k !== undefined) {
      config.topK = parameters.top_k;
    }
    if (parameters.stop !== undefined) {
      config.stopSequences = Array.isArray(parameters.stop) ? parameters.stop : [parameters.stop];
    }

    // response_format -> Gemini response schema / mime type
    const responseFormat = parameters.response_format;
    if (responseFormat) {
      const format = typeof responseFormat === 'string' ? { type: responseFormat } : responseFormat;
      const formatType = typeof format?.type === 'string' ? format.type : null;

      if (formatType === 'json_object' || formatType === 'json_schema') {
        config.responseMimeType = 'application/json';
      }

      if (formatType === 'json_schema') {
        const schema =
          format?.json_schema?.schema ||
          format?.json_schema ||
          format?.schema;
        if (schema && typeof schema === 'object') {
          config.responseSchema = cleanJsonSchema(schema);
        }
      }
    }

    // 处理 reasoning effort -> thinking config
    // 支持多种格式：
    // 1. reasoning_effort (顶级字段，OpenAI 官方格式)
    // 2. reasoning.effort (嵌套对象，Claude Code CLI 格式)
    // 3. 模型名包含 -thinking 时使用默认值
    const effort = this.getReasoningEffort(parameters, modelName);

    if (effort) {
      // 判断是否使用 thinkingLevel（Gemini 3 系列）还是 thinkingBudget
      const useLevel = shouldUseThinkingLevel(modelName);

      if (useLevel) {
        // Gemini 3 系列使用 thinkingLevel
        // 将 effort 映射为 thinkingLevel
        const levelMap = { low: 'LOW', medium: 'MEDIUM', high: 'HIGH' };
        config.thinkingConfig = {
          includeThoughts: true,
          thinkingLevel: levelMap[effort] || 'MEDIUM'
        };
      } else {
        // 其他模型使用 thinkingBudget
        const rawBudget = resolveThinkingBudget(effort);
        const maxOutputTokens = config.maxOutputTokens || 0;

        // 规范化 budget：应用 min/max 限制
        const normalizedBudget = normalizeThinkingBudget(modelName, rawBudget, maxOutputTokens);

        if (normalizedBudget !== null) {
          config.thinkingConfig = {
            includeThoughts: true,
            thinkingBudget: normalizedBudget
          };
        }
        // 如果 normalizedBudget 为 null，表示 budget 低于最小值，不启用 thinking
      }
    }

    return config;
  }
}

export default OpenAIToGeminiRequestConverter;
