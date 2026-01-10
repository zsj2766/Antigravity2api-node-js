/**
 * OpenAI → Claude 请求转换器
 *
 * 输入: OpenAI Chat Completions API 请求格式
 * 输出: Claude Messages API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import { generateToolUseId, resolveThinkingBudget, DATA_URL_REGEX } from '../common/index.js';
import { safeJsonParse } from '../../utils/utils.js';

export class OpenAIToClaudeRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   *
   * @param {object} body - OpenAI Chat Completions API 请求体
   * @param {object} context - 上下文信息
   * @returns {Promise<object>} Claude Messages API 请求体
   */
  async convert(body, context = {}) {
    if (!body || typeof body !== 'object') {
      throw new Error('请求体格式不合法');
    }
    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      throw new Error('messages 不能为空');
    }

    // 1. 规范化消息（提取 system，合并连续角色，确保 user 开头）
    const { system, messages: normalizedMessages } = this.normalizeMessagesForClaude(body.messages);

    // 2. 转换消息内容
    const claudeMessages = this.convertMessages(normalizedMessages);

    // 3. 确保 user/assistant 交替
    const finalMessages = this.ensureAlternatingRoles(claudeMessages);

    const maxTokens = body.max_tokens ?? body.max_completion_tokens ?? 10000;

    const result = {
      model: body.model,
      max_tokens: maxTokens,
      messages: finalMessages,
      stream: body.stream === true
    };

    // 添加 system
    if (system) {
      result.system = system;
    }

    // 添加工具
    if (body.tools && body.tools.length > 0) {
      result.tools = this.convertTools(body.tools);
      result.tool_choice = this.convertToolConfig(body.tool_choice, body.tools);
    }

    // 添加可选参数
    if (body.temperature !== undefined) {
      result.temperature = body.temperature;
    }
    if (body.top_p !== undefined) {
      result.top_p = body.top_p;
    }

    // 处理 reasoning effort -> thinking budget
    // 支持两种格式：
    // 1. reasoning: { effort: 'low'|'medium'|'high' } (OpenAI 结构化格式)
    // 2. reasoning_effort: 'low'|'medium'|'high' (OpenAI 简化格式)
    let effort = null;
    if (body.reasoning && typeof body.reasoning === 'object') {
      effort = body.reasoning.effort;
    } else if (typeof body.reasoning_effort === 'string') {
      effort = body.reasoning_effort;
    }

    if (effort) {
      const budgetTokens = resolveThinkingBudget(effort);

      result.thinking = {
        type: 'enabled',
        budget_tokens: budgetTokens
      };

      // 确保 max_tokens 大于 budget_tokens
      if (result.max_tokens <= budgetTokens) {
        result.max_tokens = budgetTokens + 10000;
      }

      // 当启用 thinking 时，确保所有 assistant 消息都以 thinking block 开头
      // Claude API 要求：启用 thinking 后，assistant 消息必须以 thinking/redacted_thinking 开头
      this.ensureThinkingBlocks(result.messages);
    }

    return result;
  }

  /**
   * 转换消息数组 (OpenAI → Claude)
   *
   * @param {Array} messages - OpenAI 消息数组
   * @returns {Array} Claude messages 数组
   */
  convertMessages(messages) {
    const claudeMessages = [];

    for (const msg of messages) {
      if (msg.role === 'system') {
        // system 消息由 normalizeMessagesForClaude 处理
        continue;
      }

      if (msg.role === 'user') {
        claudeMessages.push({
          role: 'user',
          content: this.convertContent(msg.content)
        });
      } else if (msg.role === 'assistant') {
        const content = [];

        // 添加 thinking block（OpenAI reasoning_content -> Claude thinking）
        // 必须放在最前面，Claude 要求 assistant 消息以 thinking 开头
        if (msg.reasoning_content) {
          const thinkingBlock = {
            type: 'thinking',
            thinking: msg.reasoning_content
          };
          // 如果有签名则添加（用于多轮对话时保持签名一致性）
          if (msg.reasoning_signature) {
            thinkingBlock.signature = msg.reasoning_signature;
          }
          content.push(thinkingBlock);
        }

        // 添加文本内容
        if (msg.content) {
          content.push(...this.convertContent(msg.content));
        }

        // 添加工具调用
        if (msg.tool_calls && msg.tool_calls.length > 0) {
          content.push(...this.convertToolCalls(msg.tool_calls));
        }

        if (content.length > 0) {
          claudeMessages.push({
            role: 'assistant',
            content
          });
        }
      } else if (msg.role === 'tool') {
        // tool 消息作为 user 角色的 tool_result
        const lastMsg = claudeMessages[claudeMessages.length - 1];
        const toolResult = this.convertToolResult(msg);

        if (lastMsg && lastMsg.role === 'user') {
          if (!Array.isArray(lastMsg.content)) {
            lastMsg.content = [{ type: 'text', text: lastMsg.content || '' }];
          }
          lastMsg.content.push(toolResult);
        } else {
          claudeMessages.push({
            role: 'user',
            content: [toolResult]
          });
        }
      }
    }

    return claudeMessages;
  }

  /**
   * 转换内容块 (OpenAI content → Claude content blocks)
   *
   * 注意：始终返回 Claude 内容块数组格式
   *
   * @param {string|Array} content - OpenAI 内容（字符串或内容块数组）
   * @returns {Array} Claude 内容块数组
   */
  convertContent(content) {
    if (typeof content === 'string') {
      return [{ type: 'text', text: content }];
    }

    if (!Array.isArray(content)) {
      return [{ type: 'text', text: '' }];
    }

    const blocks = [];

    for (const part of content) {
      if (!part || typeof part !== 'object') continue;

      switch (part.type) {
        case 'text':
          if (part.text) {
            blocks.push({ type: 'text', text: part.text });
          }
          break;

        case 'image_url':
          const imageBlock = this.convertImage(part.image_url);
          if (imageBlock) {
            blocks.push(imageBlock);
          }
          break;

        case 'file':
          const fileBlock = this.convertDocument(part.file);
          if (fileBlock) {
            blocks.push(fileBlock);
          }
          break;

        case 'input_audio':
          const audioBlock = this.convertAudio(part);
          if (audioBlock) {
            blocks.push(audioBlock);
          }
          break;

        case 'input_file':
          // OpenAI Responses API input_file 格式
          // 支持 file_data (base64/URL)，file_id 降级为占位符
          const inputFileBlock = this.convertInputFile(part);
          if (inputFileBlock) {
            blocks.push(inputFileBlock);
          }
          break;
      }
    }

    return blocks.length > 0 ? blocks : [{ type: 'text', text: '' }];
  }

  /**
   * 转换文本内容
   *
   * @param {string|object} content - 文本内容
   * @returns {object} Claude 文本块 { type: 'text', text: string }
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { type: 'text', text: content };
    }
    return { type: 'text', text: content?.text || '' };
  }

  /**
   * 转换图片内容 (OpenAI → Claude)
   *
   * @param {object} imageUrl - OpenAI image_url 对象 { url: string }
   * @returns {object|null} Claude image 块，不支持的格式返回 null
   */
  convertImage(imageUrl) {
    const url = imageUrl?.url;
    if (!url) return null;

    // base64 Data URL
    const match = url.match(DATA_URL_REGEX);
    if (match) {
      return {
        type: 'image',
        source: {
          type: 'base64',
          media_type: match[1],
          data: match[2]
        }
      };
    }

    // 普通 URL
    if (url.startsWith('http://') || url.startsWith('https://')) {
      return {
        type: 'image',
        source: {
          type: 'url',
          url: url
        }
      };
    }

    return null;
  }

  /**
   * 转换文档内容 (OpenAI → Claude)
   *
   * @param {object} filePart - OpenAI file 对象
   * @returns {object|null} Claude document 块，不支持的格式返回 null
   */
  convertDocument(filePart) {
    if (!filePart) return null;

    // file_data (Data URL)
    if (filePart.file_data) {
      const match = filePart.file_data.match(DATA_URL_REGEX);
      if (match) {
        const documentBlock = {
          type: 'document',
          source: {
            type: 'base64',
            media_type: match[1],
            data: match[2]
          }
        };

        const docTitle = filePart.title || filePart.filename;
        if (docTitle) {
          documentBlock.title = docTitle;
        }

        if (filePart.context) {
          documentBlock.context = filePart.context;
        }

        return documentBlock;
      }
    }
    // file_id 引用 - Claude 不支持，返回文本占位符
    else if (filePart.file_id) {
      return {
        type: 'text',
        text: `[File reference not supported: ${filePart.file_id}]`
      };
    }

    return null;
  }

  /**
   * 转换音频内容 (OpenAI → Claude)
   *
   * Claude Messages API 不支持音频输入，降级为文本占位符。
   *
   * @param {object} content - OpenAI input_audio 块
   * @returns {object|null} Claude 文本块
   */
  convertAudio(content) {
    const audio = content?.input_audio;
    if (!audio?.data) return null;
    return { type: 'text', text: '[Audio input not supported by Claude]' };
  }

  /**
   * 转换 input_file 内容 (OpenAI → Claude)
   *
   * OpenAI Responses API 的 input_file 格式，支持 file_data (base64/URL)。
   * file_id 引用 Claude 不支持，降级为占位符。
   *
   * @param {object} part - OpenAI input_file 块
   * @returns {object|null} Claude document 块或文本占位符
   */
  convertInputFile(part) {
    if (!part) return null;

    // file_data (base64 Data URL)
    if (part.file_data) {
      const match = part.file_data.match(DATA_URL_REGEX);
      if (match) {
        const mimeType = match[1];
        // 图片类型
        if (mimeType.startsWith('image/')) {
          return {
            type: 'image',
            source: {
              type: 'base64',
              media_type: mimeType,
              data: match[2]
            }
          };
        }
        // 文档类型
        return {
          type: 'document',
          source: {
            type: 'base64',
            media_type: mimeType,
            data: match[2]
          },
          title: part.filename || undefined
        };
      }

      // URL 格式
      if (part.file_data.startsWith('http://') || part.file_data.startsWith('https://')) {
        // 根据文件名推断类型
        const isImage = part.filename && /\.(jpg|jpeg|png|gif|webp|bmp)$/i.test(part.filename);
        if (isImage) {
          return {
            type: 'image',
            source: {
              type: 'url',
              url: part.file_data
            }
          };
        }
        return {
          type: 'document',
          source: {
            type: 'url',
            url: part.file_data,
            media_type: 'application/pdf'
          },
          title: part.filename || undefined
        };
      }
    }

    // file_id 引用 - Claude 不支持
    if (part.file_id) {
      return {
        type: 'text',
        text: `[File reference not supported: ${part.file_id}]`
      };
    }

    return null;
  }

  /**
   * 转换工具定义 (OpenAI → Claude)
   *
   * @param {Array} tools - OpenAI function 工具定义数组
   * @returns {Array} Claude 工具定义数组
   */
  convertTools(tools) {
    if (!tools || !Array.isArray(tools) || tools.length === 0) {
      return [];
    }

    return tools.map(tool => {
      // 跳过非 function 类型
      if (tool.type && tool.type !== 'function') return null;

      const func = tool.function || tool;
      if (!func || !func.name) return null;

      return {
        name: func.name,
        description: func.description || '',
        input_schema: func.parameters || { type: 'object', properties: {} }
      };
    }).filter(Boolean);
  }

  /**
   * 转换工具选择配置 (OpenAI → Claude)
   *
   * @param {string|object} toolChoice - OpenAI tool_choice 配置
   * @param {Array} tools - 工具列表
   * @returns {object} Claude tool_choice 配置
   */
  convertToolConfig(toolChoice, tools) {
    if (!toolChoice) {
      return { type: 'auto' };
    }

    if (typeof toolChoice === 'string') {
      switch (toolChoice) {
        case 'auto':
          return { type: 'auto' };
        case 'none':
          return { type: 'none' };
        case 'required':
          return { type: 'any' };
        default:
          return { type: 'auto' };
      }
    }

    if (toolChoice.type === 'function' && toolChoice.function?.name) {
      return {
        type: 'tool',
        name: toolChoice.function.name
      };
    }

    return { type: 'auto' };
  }

  /**
   * 转换工具调用 (OpenAI tool_calls → Claude tool_use blocks)
   *
   * @param {Array} toolCalls - OpenAI tool_calls 数组
   * @returns {Array} Claude tool_use 块数组
   */
  convertToolCalls(toolCalls) {
    if (!Array.isArray(toolCalls)) return [];

    return toolCalls.map(tc => {
      const args = safeJsonParse(tc?.function?.arguments, {});
      return {
        type: 'tool_use',
        id: tc.id || generateToolUseId(),
        name: tc.function?.name || 'unknown',
        input: args
      };
    });
  }

  /**
   * 转换工具调用结果 (OpenAI tool message → Claude tool_result block)
   *
   * @param {object} message - OpenAI tool 消息
   * @returns {object} Claude tool_result 块
   */
  convertToolResult(message) {
    const rawContent = message.content;
    let isError = false;

    // 错误检测
    if (typeof rawContent === 'string') {
      isError = /^error:\s*/i.test(rawContent);
    } else if (Array.isArray(rawContent) && rawContent.length > 0) {
      const firstText = rawContent.find(b => b.type === 'text');
      if (firstText && typeof firstText.text === 'string') {
        isError = /^error:\s*/i.test(firstText.text);
      }
    }

    const content = this.convertContent(rawContent);

    const result = {
      type: 'tool_result',
      tool_use_id: message.tool_call_id,
      content: content
    };

    if (isError) {
      result.is_error = true;
    }

    return result;
  }

  /**
   * 确保消息交替出现（Claude 要求 user/assistant 交替）
   *
   * @param {Array} messages - Claude 消息数组
   * @returns {Array} 确保交替后的消息数组
   */
  ensureAlternatingRoles(messages) {
    if (!messages || messages.length === 0) {
      return [{ role: 'user', content: [{ type: 'text', text: '' }] }];
    }

    const result = [];
    let lastRole = null;

    for (const msg of messages) {
      if (msg.role === lastRole) {
        const lastMsg = result[result.length - 1];
        if (Array.isArray(lastMsg.content) && Array.isArray(msg.content)) {
          lastMsg.content.push(...msg.content);
        }
      } else {
        result.push({ ...msg });
        lastRole = msg.role;
      }
    }

    // 确保以 user 开头
    if (result.length > 0 && result[0].role !== 'user') {
      result.unshift({
        role: 'user',
        content: [{ type: 'text', text: '[Conversation start]' }]
      });
    }

    return result;
  }

  /**
   * 规范化 OpenAI 消息为 Claude 格式
   *
   * 处理: 提取 system/developer 消息、合并连续同角色消息、确保 user 开头
   *
   * @param {Array} messages - OpenAI 消息数组
   * @returns {{ system: string, messages: Array }} 提取的 system 和规范化后的消息
   */
  normalizeMessagesForClaude(messages) {
    if (!Array.isArray(messages)) {
      return {
        system: '',
        messages: [{ role: 'user', content: [{ type: 'text', text: '' }] }]
      };
    }

    // 1. 提取 system/developer 消息
    const systemParts = [];
    const filteredMessages = [];

    for (const msg of messages) {
      if (msg.role === 'system' || msg.role === 'developer') {
        if (typeof msg.content === 'string') {
          systemParts.push(msg.content);
        } else if (Array.isArray(msg.content)) {
          const text = msg.content
            .filter(p => p.type === 'text')
            .map(p => p.text || '')
            .join('\n');
          if (text) systemParts.push(text);
        }
      } else {
        filteredMessages.push(msg);
      }
    }

    // 2. 合并连续同角色消息
    const merged = [];
    let current = null;

    for (const msg of filteredMessages) {
      if (!msg || !msg.role) continue;

      // tool 消息保持原样
      if (msg.role === 'tool') {
        if (current) {
          merged.push(current);
          current = null;
        }
        merged.push({
          role: 'tool',
          content: this.normalizeContentArray(msg.content),
          tool_call_id: msg.tool_call_id
        });
        continue;
      }

      // 规范化角色
      const role = msg.role === 'assistant' ? 'assistant' : 'user';

      if (current && current.role === role) {
        // 合并内容
        const existingArr = this.normalizeContentArray(current.content);
        const newArr = this.normalizeContentArray(msg.content);
        current.content = [...existingArr, ...newArr];
      } else {
        if (current) merged.push(current);
        current = {
          role,
          content: this.normalizeContentArray(msg.content)
        };
      }
    }

    if (current) merged.push(current);

    // 3. 确保 user 开头
    let normalized = merged;
    if (normalized.length === 0) {
      normalized = [{ role: 'user', content: [{ type: 'text', text: '' }] }];
    } else if (normalized[0].role !== 'user') {
      normalized = [
        { role: 'user', content: [{ type: 'text', text: '[Conversation start]' }] },
        ...normalized
      ];
    }

    return {
      system: systemParts.join('\n\n'),
      messages: normalized
    };
  }

  /**
   * 规范化内容为数组格式
   *
   * @param {string|Array} content - 内容（字符串或数组）
   * @returns {Array} 规范化后的内容数组
   */
  normalizeContentArray(content) {
    if (typeof content === 'string') {
      return [{ type: 'text', text: content }];
    }
    if (Array.isArray(content)) {
      return content;
    }
    return [{ type: 'text', text: '' }];
  }

  /**
   * 确保所有 assistant 消息都以 thinking block 开头
   *
   * Claude API 要求：当启用 thinking 时，assistant 消息必须以 thinking 或 redacted_thinking 开头。
   * 如果历史消息中的 assistant 没有 thinking block（比如之前的对话未启用 thinking），
   * 需要添加一个 redacted_thinking block 作为占位符。
   *
   * @param {Array} messages - Claude messages 数组（会被原地修改）
   */
  ensureThinkingBlocks(messages) {
    if (!Array.isArray(messages)) return;

    for (const msg of messages) {
      if (msg.role !== 'assistant') continue;
      if (!Array.isArray(msg.content) || msg.content.length === 0) continue;

      const firstBlock = msg.content[0];
      // 检查是否已有 thinking 或 redacted_thinking block
      if (firstBlock.type === 'thinking' || firstBlock.type === 'redacted_thinking') {
        continue;
      }

      // 需要在开头添加 redacted_thinking block
      // redacted_thinking 表示思考内容被删除/不可用，但满足 Claude API 的格式要求
      msg.content.unshift({
        type: 'redacted_thinking',
        data: 'kMPE'  // 最小有效 base64 占位符
      });
    }
  }
}

export default OpenAIToClaudeRequestConverter;
