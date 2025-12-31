/**
 * Claude → Gemini 请求转换器
 *
 * 输入: Claude Messages API 请求格式
 * 输出: Gemini API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import {
  convertClaudeImageToGemini,
  convertClaudeDocumentToGemini,
  extractMediaFromToolResult
} from '../../utils/converters/imageUtils.js';
import { cleanJsonSchema } from '../../utils/converters/schemaUtils.js';
import { generateToolUseId } from '../../utils/idGenerator.js';
import { isThinkingModel, getThoughtSignature, getTextThoughtSignature, safeJsonParse } from '../../utils/utils.js';

// 正则常量
const INVOKE_REGEX = /<invoke\b[^>]*>[\s\S]*?<\/invoke>/gi;
const TOOL_RESULT_REGEX = /<tool_result\b[^>]*>[\s\S]*?<\/tool_result>/gi;

export class ClaudeToGeminiRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   */
  async convert(body, context = {}) {
    const { messages, system, tools, tool_choice, ...parameters } = body;
    const modelName = context.model || body.model || 'gemini-pro';
    const enableThinking = isThinkingModel(modelName);

    const contents = this.convertMessages(messages, modelName, enableThinking);
    const geminiTools = this.convertTools(tools);
    const toolConfig = this.convertToolConfig(tool_choice, tools);

    // 提取系统指令
    const systemInstruction = this.extractSystemInstruction(system);

    // 确保消息角色交替（Gemini 强制要求 User/Model 交替）
    const mergedContents = this.mergeConsecutiveRoles(contents);

    // 构建请求体
    const requestBody = {
      contents: mergedContents,
      generationConfig: this.buildGenerationConfig(parameters, modelName)
    };

    if (systemInstruction) {
      requestBody.systemInstruction = { parts: [{ text: systemInstruction }] };
    }

    if (geminiTools.length > 0) {
      requestBody.tools = geminiTools;
      requestBody.toolConfig = { functionCallingConfig: toolConfig };
    }

    return requestBody;
  }

  /**
   * 转换消息数组
   */
  convertMessages(messages, modelName, enableThinking = false) {
    if (!Array.isArray(messages)) return [];

    const contents = [];

    for (const message of messages) {
      const parsed = this.parseContentBlocks(message.content);

      if (message.role === 'user') {
        this.handleUserMessage(parsed, contents, enableThinking);
      } else if (message.role === 'assistant') {
        this.handleAssistantMessage(parsed, contents, modelName);
      }
    }

    return contents;
  }

  /**
   * 解析 Claude 消息内容块
   */
  parseContentBlocks(content) {
    const result = {
      textParts: [],
      thinkingParts: [],
      toolCalls: [],
      toolResults: [],
      images: [],
      documents: []
    };

    if (typeof content === 'string') {
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
          if (block.thinking) {
            result.thinkingParts.push({
              thinking: block.thinking,
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
   * 处理 user 消息
   */
  handleUserMessage(parsed, contents, enableThinking) {
    // 先处理 tool_result
    for (const toolResult of parsed.toolResults) {
      let functionName = '';
      for (let i = contents.length - 1; i >= 0; i--) {
        if (contents[i].role === 'model') {
          const parts = contents[i].parts;
          for (const part of parts) {
            if (part?.functionCall?.id === toolResult.tool_use_id) {
              functionName = part.functionCall.name;
              break;
            }
          }
          if (functionName) break;
        }
      }

      let outputContent = toolResult.content;
      if (toolResult.is_error) {
        outputContent = `Error: ${outputContent}`;
      }

      const responseParts = [{
        functionResponse: {
          id: toolResult.tool_use_id,
          name: functionName,
          response: { output: outputContent }
        }
      }];

      if (toolResult.images?.length > 0) {
        responseParts.push(...toolResult.images);
      }
      if (toolResult.documents?.length > 0) {
        responseParts.push(...toolResult.documents);
      }

      const lastMessage = contents[contents.length - 1];
      if (lastMessage?.role === 'user' && lastMessage.parts.some(p => p.functionResponse)) {
        lastMessage.parts.push(...responseParts);
      } else {
        contents.push({ role: 'user', parts: responseParts });
      }
    }

    // 处理文本、图片和文档
    const hasContent = parsed.textParts.length > 0 || parsed.images.length > 0 || parsed.documents.length > 0;
    if (hasContent) {
      const parts = [];

      if (parsed.textParts.length > 0) {
        let text = parsed.textParts.join('\n');
        if (enableThinking) {
          text += '<thinking_mode>interleaved</thinking_mode><max_thinking_length>16000</max_thinking_length>';
        }
        parts.push({ text });
      }

      parts.push(...parsed.images);
      parts.push(...parsed.documents);

      if (parts.length > 0) {
        contents.push({ role: 'user', parts });
      }
    }
  }

  /**
   * 处理 assistant 消息
   */
  handleAssistantMessage(parsed, contents, modelName) {
    const allowThoughtSignature = typeof modelName === 'string' && modelName.includes('gemini-3');
    const parts = [];

    // 处理 thinking 块
    for (const thinkingBlock of parsed.thinkingParts) {
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

    // 合并到上一条 model 消息
    const lastMessage = contents[contents.length - 1];
    const onlyToolCalls = parts.every(p => p.functionCall) && parts.length > 0;

    if (lastMessage?.role === 'model' && onlyToolCalls && parsed.textParts.length === 0) {
      lastMessage.parts.push(...parts);
    } else if (parts.length > 0) {
      contents.push({ role: 'model', parts });
    }
  }

  /**
   * 提取系统指令
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
   * 转换图片内容
   */
  convertImage(content) {
    return convertClaudeImageToGemini(content);
  }

  /**
   * 转换文档内容
   */
  convertDocument(content) {
    return convertClaudeDocumentToGemini(content);
  }

  /**
   * 转换工具定义
   */
  convertTools(tools) {
    if (!tools || !Array.isArray(tools) || tools.length === 0) return [];

    return tools.map(tool => {
      if (!tool || typeof tool !== 'object') return null;

      const name = tool.name;
      const description = tool.description;
      const rawParameters = tool.input_schema || {};

      const parameters = rawParameters ? JSON.parse(JSON.stringify(rawParameters)) : {};
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
   * 转换工具选择配置
   */
  convertToolConfig(toolChoice, tools) {
    if (!toolChoice) {
      return { mode: 'AUTO' };
    }

    switch (toolChoice.type) {
      case 'auto':
        return { mode: 'AUTO' };
      case 'any':
        return { mode: 'ANY' };
      case 'tool':
        return {
          mode: 'ANY',
          allowed_function_names: [toolChoice.name]
        };
      case 'none':
        return { mode: 'NONE' };
      default:
        return { mode: 'AUTO' };
    }
  }

  /**
   * 转换工具调用结果
   */
  convertToolResult(result) {
    const mediaContent = extractMediaFromToolResult(result.content);

    return {
      functionResponse: {
        id: result.tool_use_id,
        name: result.name || '',
        response: { output: mediaContent.text || '' }
      }
    };
  }

  /**
   * 合并连续相同角色的消息（Gemini 要求 user/model 严格交替）
   */
  mergeConsecutiveRoles(contents) {
    if (!contents || contents.length === 0) return [];

    const merged = [];
    for (const content of contents) {
      const last = merged[merged.length - 1];
      if (last && last.role === content.role) {
        // 合并 parts
        last.parts.push(...content.parts);
      } else {
        merged.push({ role: content.role, parts: [...content.parts] });
      }
    }
    return merged;
  }

  /**
   * 构建生成配置
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
