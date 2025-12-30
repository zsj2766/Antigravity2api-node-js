/**
 * 统一工具定义转换器
 *
 * 【请求转换】工具定义格式互转
 *
 * 支持三种格式互转：
 * - OpenAI: { type: 'function', function: { name, description, parameters } }
 * - Claude: { name, description, input_schema }
 * - Gemini: { functionDeclarations: [{ name, description, parameters }] }
 */

import { cleanJsonSchema } from '../schemaUtils.js';

/**
 * 深拷贝对象（避免修改原始数据）
 */
function safeDeepCopy(obj) {
  if (!obj || typeof obj !== 'object') return obj;
  return JSON.parse(JSON.stringify(obj));
}

/**
 * 从不同格式提取统一的工具定义
 * @param {object} tool - 工具定义
 * @param {'openai'|'claude'} sourceType - 源格式
 * @returns {{ name: string, description: string, parameters: object }}
 */
function extractToolDefinition(tool, sourceType) {
  if (!tool || typeof tool !== 'object') {
    return { name: '', description: '', parameters: {} };
  }

  if (sourceType === 'claude') {
    return {
      name: tool.name || '',
      description: tool.description || '',
      parameters: tool.input_schema || {}
    };
  }

  // OpenAI 格式：可能是 { type: 'function', function: {...} } 或直接 { name, description, parameters }
  const func = tool.function || tool;
  return {
    name: func.name || '',
    description: func.description || '',
    parameters: func.parameters || {}
  };
}

export const ToolConverter = {
  /**
   * 【请求转换】OpenAI/Claude 工具定义 → Gemini 格式
   *
   * 转换方向: OpenAI/Claude → Gemini
   *
   * @param {Array} tools - 源工具数组
   * @param {'openai'|'claude'} sourceType - 源格式类型
   * @returns {Array} - Gemini functionDeclarations 数组
   */
  toGemini(tools, sourceType = 'openai') {
    if (!tools || !Array.isArray(tools) || tools.length === 0) {
      return [];
    }

    return tools.map(tool => {
      const { name, description, parameters } = extractToolDefinition(tool, sourceType);

      if (!name) return null;

      // 深拷贝并清理 JSON Schema
      const cleanedParameters = cleanJsonSchema(safeDeepCopy(parameters));

      return {
        functionDeclarations: [{
          name,
          description,
          parameters: cleanedParameters
        }]
      };
    }).filter(Boolean);
  },

  /**
   * 【请求转换】Claude 工具定义 → OpenAI 格式
   *
   * 转换方向: Claude → OpenAI
   *
   * @param {Array} tools - Claude 工具数组
   * @returns {Array} - OpenAI tools 数组
   */
  toOpenAI(tools) {
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
  },

  /**
   * 【请求转换】OpenAI 工具定义 → Claude 格式
   *
   * 转换方向: OpenAI → Claude
   *
   * @param {Array} tools - OpenAI 工具数组
   * @returns {Array} - Claude tools 数组
   */
  toClaude(tools) {
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
};

export default ToolConverter;
