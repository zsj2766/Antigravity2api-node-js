/**
 * Claude → OpenAI 请求转换器
 *
 * 输入: Claude Messages API 请求格式
 * 输出: OpenAI Chat Completions API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import { generateToolCallId } from '../../utils/idGenerator.js';
import { ToolConverter } from '../../utils/converters/common/toolConverter.js';
import { convertClaudeImageToOpenAI, extractMediaFromToolResult } from '../../utils/converters/imageUtils.js';
import { resolveReasoningEffort } from '../../utils/converters/thinkingConfig.js';
import { safeJsonStringify } from '../../utils/utils.js';

export class ClaudeToOpenAIRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
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
        const toolResults = this.extractToolResults(message.content);

        if (toolResults.length > 0) {
          // 将 tool_result 转为 OpenAI tool 消息
          for (const tr of toolResults) {
            let content = tr.content;
            if (tr.is_error) {
              content = `Error: ${content}`;
            }
            messages.push({
              role: 'tool',
              tool_call_id: tr.tool_use_id,
              content: content
            });
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

    const result = {
      model: body.model,
      stream: body.stream !== false,
      temperature: body.temperature ?? 0.2,
      top_p: body.top_p ?? 1,
      max_tokens: body.max_tokens,
      messages
    };

    // 处理 thinking -> reasoning_effort
    if (body.thinking && body.thinking.type === 'enabled' && body.thinking.budget_tokens) {
      result.reasoning_effort = resolveReasoningEffort(body.thinking.budget_tokens);
    }

    // 添加工具定义
    if (body.tools && body.tools.length > 0) {
      result.tools = this.convertTools(body.tools);
      result.tool_choice = this.convertToolConfig(body.tool_choice, body.tools);
    }

    return result;
  }

  /**
   * 转换内容
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
        case 'redacted_thinking':
          // OpenAI 不支持 thinking，忽略
          break;

        case 'tool_use':
          // 已通过 extractToolCalls 处理
          break;

        case 'tool_result':
          // 处理嵌套图片
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
          hasMultimodal = true;
          const docBlock = this.convertDocument(block);
          if (docBlock) {
            parts.push(docBlock);
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
   * 转换文本内容
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
   * 转换图片内容
   */
  convertImage(content) {
    return convertClaudeImageToOpenAI(content);
  }

  /**
   * 转换文档内容
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
      return {
        type: 'file',
        file: {
          filename: filename,
          file_data: docSource.url
        }
      };
    }

    return null;
  }

  /**
   * 转换工具定义
   */
  convertTools(tools) {
    return ToolConverter.toOpenAI(tools);
  }

  /**
   * 转换工具选择配置
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
   * 提��工具调用
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
   * 提取工具结果
   */
  extractToolResults(content) {
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
   * 转换工具调用结果
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
