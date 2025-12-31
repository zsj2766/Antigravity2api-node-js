/**
 * OpenAI → Gemini 请求转换器
 *
 * 输入: OpenAI Chat Completions API 请求格式
 * 输出: Gemini API 请求格式
 */

import { IRequestConverter } from '../interfaces/IRequestConverter.js';
import { extractImagesFromContent } from '../../utils/converters/imageUtils.js';
import { cleanJsonSchema } from '../../utils/converters/schemaUtils.js';

// Data URL 正则
const DATA_URL_REGEX = /^data:([^;]+);base64,(.+)$/;

export class OpenAIToGeminiRequestConverter extends IRequestConverter {
  /**
   * 主入口：转换完整请求体
   */
  async convert(body, context = {}) {
    const { messages, tools, tool_choice, ...parameters } = body;
    const modelName = context.model || body.model || 'gemini-pro';

    const contents = this.convertMessages(messages, modelName);
    const geminiTools = this.convertTools(tools);
    const toolConfig = this.convertToolConfig(tool_choice, tools);

    // 提取系统指令
    const systemInstruction = this.extractSystemInstruction(messages);

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
  convertMessages(messages, modelName) {
    if (!Array.isArray(messages)) return [];

    const contents = [];

    for (const message of messages) {
      if (message.role === 'system') {
        // system 消息单独处理为 systemInstruction，这里跳过
        continue;
      }

      if (message.role === 'user') {
        const extracted = extractImagesFromContent(message.content);
        const parts = this.buildUserParts(extracted);
        contents.push({ role: 'user', parts });
      } else if (message.role === 'assistant') {
        const parts = this.buildAssistantParts(message);
        contents.push({ role: 'model', parts });
      } else if (message.role === 'tool') {
        const parts = this.buildToolResultParts(message, contents);
        contents.push({ role: 'user', parts });
      }
    }

    return contents;
  }

  /**
   * 构建用户消息的 parts
   */
  buildUserParts(extracted) {
    const parts = [];

    if (extracted.text && extracted.text.trim()) {
      parts.push({ text: extracted.text });
    }

    if (extracted.images && extracted.images.length > 0) {
      parts.push(...extracted.images);
    }

    if (extracted.documents && extracted.documents.length > 0) {
      parts.push(...extracted.documents);
    }

    if (parts.length === 0) {
      parts.push({ text: '' });
    }

    return parts;
  }

  /**
   * 构建助手消息的 parts
   */
  buildAssistantParts(message) {
    const parts = [];

    // 处理文本内容
    if (message.content) {
      if (typeof message.content === 'string') {
        parts.push({ text: message.content });
      } else if (Array.isArray(message.content)) {
        const textContent = message.content
          .filter(item => item.type === 'text')
          .map(item => item.text || '')
          .join('');
        if (textContent) {
          parts.push({ text: textContent });
        }
      }
    }

    // 处理工具调用
    if (message.tool_calls && message.tool_calls.length > 0) {
      for (const toolCall of message.tool_calls) {
        let args = {};
        try {
          args = JSON.parse(toolCall.function?.arguments || '{}');
        } catch (e) {
          args = {};
        }

        parts.push({
          functionCall: {
            id: toolCall.id,
            name: toolCall.function?.name,
            args
          }
        });
      }
    }

    return parts;
  }

  /**
   * 构建工具结果的 parts
   */
  buildToolResultParts(message, contents) {
    // 查找对应的函数名
    let functionName = '';
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

    const extracted = extractImagesFromContent(message.content);
    const parts = [];

    parts.push({
      functionResponse: {
        id: message.tool_call_id,
        name: functionName,
        response: {
          output: extracted.text || ''
        }
      }
    });

    // 添加图片和文档
    if (extracted.images?.length > 0) {
      parts.push(...extracted.images);
    }
    if (extracted.documents?.length > 0) {
      parts.push(...extracted.documents);
    }

    return parts;
  }

  /**
   * 提取系统指令
   */
  extractSystemInstruction(messages) {
    if (!Array.isArray(messages)) return null;

    const systemMessages = messages.filter(m => m.role === 'system');
    if (systemMessages.length === 0) return null;

    return systemMessages
      .map(m => typeof m.content === 'string' ? m.content : '')
      .filter(Boolean)
      .join('\n\n');
  }

  /**
   * 转换文本内容
   */
  convertText(content) {
    if (typeof content === 'string') {
      return { text: content };
    }
    return { text: content?.text || '' };
  }

  /**
   * 转换图片内容
   */
  convertImage(content) {
    // OpenAI: { type: 'image_url', image_url: { url: '...' } }
    const url = content?.image_url?.url || content?.url || '';

    const match = url.match(DATA_URL_REGEX);
    if (match) {
      return {
        inlineData: {
          mimeType: match[1],
          data: match[2]
        }
      };
    }

    if (url.startsWith('http://') || url.startsWith('https://')) {
      return {
        fileData: {
          fileUri: url,
          mimeType: 'image/jpeg'
        }
      };
    }

    return null;
  }

  /**
   * 转换文档内容
   */
  convertDocument(content) {
    const fileData = content?.file?.file_data || '';

    const match = fileData.match(DATA_URL_REGEX);
    if (match) {
      return {
        inlineData: {
          mimeType: match[1],
          data: match[2]
        }
      };
    }

    return null;
  }

  /**
   * 转换工具定义
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

    if (typeof toolChoice === 'string') {
      switch (toolChoice) {
        case 'auto':
          return { mode: 'AUTO' };
        case 'none':
          return { mode: 'NONE' };
        case 'required':
          return { mode: 'ANY' };
        default:
          return { mode: 'AUTO' };
      }
    }

    if (toolChoice.type === 'function' && toolChoice.function?.name) {
      return {
        mode: 'ANY',
        allowed_function_names: [toolChoice.function.name]
      };
    }

    return { mode: 'AUTO' };
  }

  /**
   * 转换工具调用结果
   */
  convertToolResult(result) {
    return {
      functionResponse: {
        id: result.tool_call_id,
        name: result.name || '',
        response: {
          output: typeof result.content === 'string' ? result.content : JSON.stringify(result.content)
        }
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
    if (parameters.stop !== undefined) {
      config.stopSequences = Array.isArray(parameters.stop) ? parameters.stop : [parameters.stop];
    }

    return config;
  }
}

export default OpenAIToGeminiRequestConverter;
