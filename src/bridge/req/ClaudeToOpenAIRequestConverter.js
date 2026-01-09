/**
 * Claude → OpenAI 请求转换器
 *
 * 输入: Claude Messages API 请求格式
 * 输出: OpenAI Chat Completions API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import { generateToolCallId, resolveReasoningEffort, unpackThinkingText } from '../common/index.js';
import { safeJsonStringify } from '../../utils/utils.js';

export class ClaudeToOpenAIRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   *
   * @param {object} body - Claude Messages API 请求体
   * @param {object} context - 上下文信息
   * @returns {Promise<object>} OpenAI Chat Completions API 请求体
   */
  async convert(body, context = {}) {
    if (!body || typeof body !== 'object') {
      throw new Error('请求体格式不合法');
    }
    if (typeof body.max_tokens !== 'number' || Number.isNaN(body.max_tokens)) {
      throw new Error('max_tokens 是必填数字');
    }
    if (!Array.isArray(body.messages) || body.messages.length === 0) {
      throw new Error('messages 不能为空');
    }

    // 转换消息
    const messages = this.convertMessages(body.messages, body.system);

    const result = {
      model: body.model,
      stream: body.stream === true,
      temperature: body.temperature ?? 0.2,
      top_p: body.top_p ?? 1,
      max_tokens: body.max_tokens,
      messages
    };

    // 处理 thinking -> reasoning (OpenAI o1/o3 格式)
    if (body.thinking && body.thinking.type === 'enabled' && body.thinking.budget_tokens) {
      const effort = resolveReasoningEffort(body.thinking.budget_tokens);
      result.reasoning = { effort };
    }

    // 添加工具定义
    if (body.tools && body.tools.length > 0) {
      result.tools = this.convertTools(body.tools);
      result.tool_choice = this.convertToolConfig(body.tool_choice, body.tools);
    }

    return result;
  }

  /**
   * 转换消息数组 (Claude → OpenAI)
   *
   * @param {Array} claudeMessages - Claude 消息数组
   * @param {string|Array} system - Claude system 内容
   * @returns {Array} OpenAI messages 数组
   */
  convertMessages(claudeMessages, system) {
    const messages = [];

    // 处理 system 消息
    if (system) {
      const systemContent = Array.isArray(system)
        ? system
            .map(block => {
              if (typeof block === 'string') return block;
              if (block && typeof block === 'object' && 'text' in block) {
                return block.text || '';
              }
              return '';
            })
            .join('\n')
        : system;
      messages.push({ role: 'system', content: systemContent });
    }

    // 收集所有 tool_use 的 id -> name 映射
    const toolUseNameMap = new Map();
    for (const message of claudeMessages) {
      if (message.role === 'assistant' && Array.isArray(message.content)) {
        for (const block of message.content) {
          if (block?.type === 'tool_use' && block.id && block.name) {
            toolUseNameMap.set(block.id, block.name);
          }
        }
      }
    }

    // 转换每条消息
    for (const message of claudeMessages) {
      if (message.role === 'user') {
        const toolResults = this.extractToolResults(message.content);

        if (toolResults.length > 0) {
          // 将 tool_result 转为 OpenAI tool 消息
          for (const tr of toolResults) {
            let content = tr.content;
            if (tr.is_error) {
              content = `Error: ${content}`;
            }
            const toolMsg = {
              role: 'tool',
              tool_call_id: tr.tool_use_id,
              content: content
            };
            const toolName = toolUseNameMap.get(tr.tool_use_id);
            toolMsg.name = toolName || 'unknown_tool';
            messages.push(toolMsg);
          }

          // 如果还有其他内容，添加为用户消息
          const { content } = this.convertContent(message.content);
          if (content && (typeof content === 'string' ? content.trim() : content.length > 0)) {
            messages.push({ role: 'user', content });
          }
        } else {
          const { content } = this.convertContent(message.content);
          messages.push({ role: 'user', content });
        }
      } else if (message.role === 'assistant') {
        const { content, toolCalls } = this.convertContent(message.content);

        const assistantMsg = {
          role: 'assistant',
          content: content || null
        };

        if (toolCalls.length > 0) {
          assistantMsg.tool_calls = toolCalls;
        }

        messages.push(assistantMsg);
      }
    }

    return messages;
  }

  /**
   * 转换内容块 (Claude content → OpenAI content)
   *
   * 注意：返回对象包含解构后的 content 和 toolCalls，
   * 因为 Claude 的 content 数组可能同时包含文本和工具调用
   *
   * @param {string|Array} content - Claude 内容（字符串或内容块数组）
   * @returns {{ content: string|Array, toolCalls: Array }} 解构后的内容和工具调用
   */
  convertContent(content) {
    if (typeof content === 'string') {
      return { content: content, toolCalls: [] };
    }

    if (!Array.isArray(content)) {
      return { content: '', toolCalls: [] };
    }

    const parts = [];
    let hasMultimodal = false;
    let hasThinking = false;
    const toolCalls = this.extractToolCalls(content);

    for (const block of content) {
      if (!block || typeof block !== 'object') continue;

      switch (block.type) {
        case 'text':
          if (block.text && block.text.trim()) {
            parts.push({ type: 'text', text: block.text });
          }
          break;

        case 'thinking':
          // 非标准扩展：保留 thinking 块供后续链路使用
          // - 标准 OpenAI API 会忽略未知字段，不影响正常请求
          // - OpenAIToGemini 支持此扩展，可正确转换为 Gemini thoughtSignature
          // 注意：按 CLIProxyAPI 策略，没有有效签名的 thinking 块应该被丢弃
          // 有效签名至少 50 字符长度
          // 解包 thinking 字段（可能是字符串、{text}、{thinking} 对象）
          const thinkingText = unpackThinkingText(block.thinking);
          if (thinkingText && block.signature && block.signature.length >= 50) {
            hasThinking = true;
            parts.push({
              type: 'thinking',
              thinking: thinkingText,
              signature: block.signature
            });
          }
          // 无有效签名或空文本的 thinking 块直接丢弃，不转换为文本
          break;

        case 'redacted_thinking':
          // 非标准扩展：保留 redacted_thinking 块
          // block.data 就是签名，需要检查有效性（至少 50 字符）
          if (block.data && block.data.length >= 50) {
            hasThinking = true;
            parts.push({
              type: 'thinking',
              thinking: '[redacted]',
              signature: block.data,
              redacted: true
            });
          }
          // 无有效签名的 redacted_thinking 块直接丢弃
          break;

        case 'tool_use':
          // 已通过 extractToolCalls 处理
          break;

        case 'tool_result':
          // tool_result 已通过 extractToolResults 单独处理并转为 OpenAI tool 消息
          // 现已支持多模态内容透传（非标准扩展），供后续链路使用
          break;

        case 'image':
          hasMultimodal = true;
          const openaiImage = this.convertImage(block);
          if (openaiImage) {
            parts.push(openaiImage);
          }
          break;

        case 'document':
          hasMultimodal = true;
          // 检查是否为嵌套内容类型
          if (block?.source?.type === 'content' && Array.isArray(block.source.content)) {
            // 递归处理嵌套内容
            const { content: nestedContent } = this.convertContent(block.source.content);
            if (typeof nestedContent === 'string') {
              parts.push({ type: 'text', text: nestedContent });
            } else if (Array.isArray(nestedContent)) {
              parts.push(...nestedContent);
            }
          } else {
            const docBlock = this.convertDocument(block);
            if (docBlock) {
              parts.push(docBlock);
            }
          }
          break;
      }
    }

    // 构建最终内容
    let finalContent;
    if (hasMultimodal || hasThinking) {
      // 有多模态或思考内容时，保留整个 parts 数组
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
   * 转换文本内容
   *
   * @param {string|object} content - 文本内容
   * @returns {object} OpenAI 文本块 { type: 'text', text: string }
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { type: 'text', text: content };
    }
    if (content?.type === 'text') {
      return { type: 'text', text: content.text || '' };
    }
    return { type: 'text', text: '' };
  }

  /**
   * 转换图片内容 (Claude → OpenAI)
   *
   * @param {object} block - Claude image 块 { source: { type, media_type, data/url } }
   * @returns {object|null} OpenAI image_url 块，不支持的格式返回 null
   */
  convertImage(block) {
    const source = block?.source;
    if (!source) return null;

    // base64 类型
    if (source.type === 'base64' && source.media_type && source.data) {
      return {
        type: 'image_url',
        image_url: {
          url: `data:${source.media_type};base64,${source.data}`,
          detail: 'auto'
        }
      };
    }

    // URL 类型
    if (source.type === 'url' && source.url) {
      return {
        type: 'image_url',
        image_url: {
          url: source.url,
          detail: 'auto'
        }
      };
    }

    return null;
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

          // 非标准扩展：转换文档供后续链路使用
          // - OpenAIToGemini 支持此扩展，可正确处理媒体内容
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
   * 转换文档内容 (Claude → OpenAI)
   *
   * @param {object} block - Claude document 块
   * @returns {object|null} OpenAI file 块或文本占位符
   */
  convertDocument(block) {
    const docSource = block?.source;
    if (!docSource) return null;

    const mediaType = docSource.media_type || 'application/pdf';
    const filename = block.title || `document.${mediaType.split('/')[1] || 'pdf'}`;

    if (docSource.type === 'base64' && docSource.data) {
      return {
        type: 'file',
        file: {
          filename: filename,
          file_data: `data:${mediaType};base64,${docSource.data}`
        }
      };
    } else if (docSource.type === 'url' && docSource.url) {
      // 非标准扩展：保留 URL 供后续链路使用
      // - 标准 OpenAI API 可能不支持，但会忽略未知格式
      // - OpenAIToGemini 支持此扩展，可正确转换为 Gemini fileData.fileUri
      return {
        type: 'file',
        file: {
          filename: filename,
          file_data: docSource.url
        }
      };
    } else if (docSource.type === 'text') {
      // Claude document source.type: 'text' - 纯文本文档
      return {
        type: 'text',
        text: docSource.data || ''
      };
    } else if (docSource.type === 'content') {
      // Claude document source.type: 'content' - 嵌套内容块
      // 在 convertContent 的 document case 中已处理递归
      // 此处返回 null，不应被调用到
      return null;
    }

    return null;
  }

  /**
   * 转换工具定义 (Claude → OpenAI)
   *
   * @param {Array} tools - Claude 工具定义数组
   * @returns {Array} OpenAI function 工具定义数组
   */
  convertTools(tools) {
    if (!tools || !Array.isArray(tools) || tools.length === 0) {
      return [];
    }

    return tools.map(tool => {
      if (!tool || !tool.name) return null;

      return {
        type: 'function',
        function: {
          name: tool.name,
          description: tool.description || '',
          parameters: tool.input_schema || { type: 'object', properties: {} }
        }
      };
    }).filter(Boolean);
  }

  /**
   * 转换工具选择配置 (Claude → OpenAI)
   *
   * @param {object} toolChoice - Claude tool_choice 配置
   * @param {Array} tools - 工具列表
   * @returns {string|object} OpenAI tool_choice 配置
   */
  convertToolConfig(toolChoice, tools) {
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
   * 转换工具调用 (Claude tool_use → OpenAI tool_calls)
   *
   * @param {Array} toolCalls - Claude tool_use 块数组
   * @returns {Array} OpenAI tool_calls 数组
   */
  convertToolCalls(toolCalls) {
    return this.extractToolCalls(Array.isArray(toolCalls) ? toolCalls : []);
  }

  /**
   * 从 content 数组中提取工具调用
   *
   * @param {Array} content - Claude content 块数组
   * @returns {Array} OpenAI tool_calls 数组
   */
  extractToolCalls(content) {
    if (!Array.isArray(content)) return [];

    return content
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
   * 从 content 数组中提取工具结果
   *
   * 注意：非标准扩展 - 返回多模态内容数组供后续链路使用
   * - 标准 OpenAI API 只支持 content: string，会忽略数组格式
   * - OpenAIToGemini 支持此扩展，可正确处理多模态工具结果
   *
   * @param {Array} content - Claude content 块数组
   * @returns {Array} 工具结果数组 { tool_use_id, content: string|Array, is_error }
   */
  extractToolResults(content) {
    if (!Array.isArray(content)) return [];

    return content
      .filter(b => b && b.type === 'tool_result')
      .map(b => {
        if (typeof b.content === 'string') {
          return {
            tool_use_id: b.tool_use_id,
            content: b.content,
            is_error: b.is_error
          };
        }

        // 使用 extractToolResultMedia 提取多模态内容
        const media = this.extractToolResultMedia(b.content);

        // 构建多模态内容数组 (非标准扩展，OpenAIToGemini 支持)
        const parts = [];

        if (media.text) {
          parts.push({ type: 'text', text: media.text });
        }

        if (media.images && media.images.length > 0) {
          parts.push(...media.images);
        }

        if (media.documents && media.documents.length > 0) {
          parts.push(...media.documents);
        }

        // 如果只有一个文本部分且不含媒体，返回字符串（符合标准 OpenAI 规范）
        // 否则返回数组（非标准扩展，但 OpenAIToGemini 支持）
        const finalContent = (parts.length === 1 && parts[0].type === 'text')
          ? parts[0].text
          : (parts.length > 0 ? parts : '');

        return {
          tool_use_id: b.tool_use_id,
          content: finalContent,
          is_error: b.is_error
        };
      });
  }

  /**
   * 转换工具调用结果 (Claude tool_result → OpenAI tool message)
   *
   * @param {object} result - Claude tool_result 块
   * @returns {object} OpenAI tool 消息
   */
  convertToolResult(result) {
    let content = typeof result.content === 'string'
      ? result.content
      : JSON.stringify(result.content || '');

    if (result.is_error) {
      content = `Error: ${content}`;
    }

    return {
      role: 'tool',
      tool_call_id: result.tool_use_id,
      content: content
    };
  }
}

export default ClaudeToOpenAIRequestConverter;
