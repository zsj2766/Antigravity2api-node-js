/**
 * OpenAI → Claude 请求转换器
 *
 * 输入: OpenAI Chat Completions API 请求格式
 * 输出: Claude Messages API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import { generateToolUseId } from '../../utils/idGenerator.js';
import { ToolConverter } from '../../utils/converters/common/toolConverter.js';
import { normalizeMessagesForClaude } from '../../utils/converters/messageUtils.js';
import { resolveThinkingBudget } from '../../utils/converters/thinkingConfig.js';
import { safeJsonParse } from '../../utils/utils.js';

// Data URL 正则
const DATA_URL_REGEX = /^data:([^;]+);base64,(.+)$/;

export class OpenAIToClaudeRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   */
  async convert(body, context = {}) {
    if (!body || typeof body !== 'object') {
      throw new Error('请求体格式不合法');
    }
    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      throw new Error('messages 不能为空');
    }

    // 1. 规范化消息（提取 system，合并连续角色，确保 user 开头）
    const { system, messages: normalizedMessages } = normalizeMessagesForClaude(body.messages);

    // 2. 转换消息内容
    const claudeMessages = this.convertMessages(normalizedMessages);

    // 3. 确保 user/assistant 交替
    const finalMessages = this.ensureAlternatingRoles(claudeMessages);

    const result = {
      model: body.model,
      max_tokens: body.max_tokens || 10000,
      messages: finalMessages,
      stream: body.stream !== false
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
    if (body.reasoning && typeof body.reasoning === 'object') {
      const budgetTokens = resolveThinkingBudget(body.reasoning.effort);

      result.thinking = {
        type: 'enabled',
        budget_tokens: budgetTokens
      };

      // 确保 max_tokens 大于 budget_tokens
      if (result.max_tokens <= budgetTokens) {
        result.max_tokens = budgetTokens + 10000;
      }
    }

    return result;
  }

  /**
   * 转换消息数组
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
   * 转换内容
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
      }
    }

    return blocks.length > 0 ? blocks : [{ type: 'text', text: '' }];
  }

  /**
   * 转换文本内容
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { type: 'text', text: content };
    }
    return { type: 'text', text: content?.text || '' };
  }

  /**
   * 转换图片内容
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
   * 转换文档内容
   */
  convertDocument(filePart) {
    if (!filePart) return null;

    let source = null;

    // file_data (Data URL)
    if (filePart.file_data) {
      const match = filePart.file_data.match(DATA_URL_REGEX);
      if (match) {
        source = {
          type: 'base64',
          media_type: match[1],
          data: match[2]
        };
      }
    }
    // file_id 引用
    else if (filePart.file_id) {
      source = {
        type: 'file',
        file_id: filePart.file_id
      };
    }

    if (!source) return null;

    const documentBlock = {
      type: 'document',
      source
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

  /**
   * 转换工具定义
   */
  convertTools(tools) {
    return ToolConverter.toClaude(tools);
  }

  /**
   * 转换工具选择配置
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
   * 转换工具调用
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
   * 转换工具调用结果
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
   * 确保消息交替出现
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
}

export default OpenAIToClaudeRequestConverter;
