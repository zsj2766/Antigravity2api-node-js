/**
 * Bridge - API 格式转换桥接层
 *
 * 统一管理 OpenAI、Claude、Gemini 三种 API 格式之间的转换
 *
 * 使用方式：
 * ```javascript
 * import { Bridge } from './bridge/index.js';
 *
 * // 获取转换器对
 * const { req, res } = await Bridge.getConverterPair('openai', 'gemini');
 *
 * // 请求转换
 * const geminiBody = await req.convert(openaiBody, context);
 *
 * // 响应转换（非流式）
 * const openaiResponse = res.convert(geminiResponse, context);
 *
 * // 响应转换（流式）
 * const processor = res.createStreamProcessor(expressRes, context);
 * // 处理每个 chunk
 * processor.process(chunk);
 * // 结束
 * processor.finish(finalChunk);
 * ```
 */

// 请求转换器
import { OpenAIToGeminiRequestConverter } from './req/OpenAIToGeminiRequestConverter.js';
import { ClaudeToGeminiRequestConverter } from './req/ClaudeToGeminiRequestConverter.js';
import { OpenAIToClaudeRequestConverter } from './req/OpenAIToClaudeRequestConverter.js';
import { ClaudeToOpenAIRequestConverter } from './req/ClaudeToOpenAIRequestConverter.js';

// 响应转换器
import { GeminiToOpenAIResponseConverter } from './res/GeminiToOpenAIResponseConverter.js';
import { GeminiToClaudeResponseConverter } from './res/GeminiToClaudeResponseConverter.js';
import { ClaudeToOpenAIResponseConverter } from './res/ClaudeToOpenAIResponseConverter.js';
import { OpenAIToClaudeResponseConverter } from './res/OpenAIToClaudeResponseConverter.js';

// 接口
export { IRequestConverter } from './interfaces/IRequestConverter.js';
export { IResponseConverter } from './interfaces/IResponseConverter.js';

/**
 * Bridge 工厂类
 */
export class Bridge {
  // 请求转换器缓存
  static requestConverters = {
    'openai->gemini': null,
    'claude->gemini': null,
    'openai->claude': null,
    'claude->openai': null
  };

  // 响应转换器缓存
  static responseConverters = {
    'gemini->openai': null,
    'gemini->claude': null,
    'claude->openai': null,
    'openai->claude': null
  };

  /**
   * 获取请求转换器
   * @param {string} clientProtocol - 客户端协议 (openai | claude)
   * @param {string} upstreamProtocol - 上游协议 (gemini | claude | openai)
   * @returns {IRequestConverter}
   */
  static getRequestConverter(clientProtocol, upstreamProtocol) {
    const key = `${clientProtocol}->${upstreamProtocol}`;

    if (!this.requestConverters[key]) {
      switch (key) {
        case 'openai->gemini':
          this.requestConverters[key] = new OpenAIToGeminiRequestConverter();
          break;
        case 'claude->gemini':
          this.requestConverters[key] = new ClaudeToGeminiRequestConverter();
          break;
        case 'openai->claude':
          this.requestConverters[key] = new OpenAIToClaudeRequestConverter();
          break;
        case 'claude->openai':
          this.requestConverters[key] = new ClaudeToOpenAIRequestConverter();
          break;
        default:
          throw new Error(`Unsupported request conversion: ${key}`);
      }
    }

    return this.requestConverters[key];
  }

  /**
   * 获取响应转换器
   * @param {string} upstreamProtocol - 上游协议 (gemini | claude | openai)
   * @param {string} clientProtocol - 客户端协议 (openai | claude)
   * @returns {IResponseConverter}
   */
  static getResponseConverter(upstreamProtocol, clientProtocol) {
    const key = `${upstreamProtocol}->${clientProtocol}`;

    if (!this.responseConverters[key]) {
      switch (key) {
        case 'gemini->openai':
          this.responseConverters[key] = new GeminiToOpenAIResponseConverter();
          break;
        case 'gemini->claude':
          this.responseConverters[key] = new GeminiToClaudeResponseConverter();
          break;
        case 'claude->openai':
          this.responseConverters[key] = new ClaudeToOpenAIResponseConverter();
          break;
        case 'openai->claude':
          this.responseConverters[key] = new OpenAIToClaudeResponseConverter();
          break;
        default:
          throw new Error(`Unsupported response conversion: ${key}`);
      }
    }

    return this.responseConverters[key];
  }

  /**
   * 获取转换器对
   * @param {string} clientProtocol - 客户端协议 (openai | claude)
   * @param {string} upstreamProtocol - 上游协议 (gemini | claude | openai)
   * @returns {{ req: IRequestConverter, res: IResponseConverter }}
   */
  static getConverterPair(clientProtocol, upstreamProtocol) {
    return {
      req: this.getRequestConverter(clientProtocol, upstreamProtocol),
      res: this.getResponseConverter(upstreamProtocol, clientProtocol)
    };
  }

  /**
   * 根据请求路径检测客户端协议
   * @param {string} path - 请求路径
   * @returns {'openai' | 'claude'}
   */
  static detectClientProtocol(path) {
    if (path.includes('/v1/messages')) {
      return 'claude';
    }
    if (path.includes('/v1/chat/completions')) {
      return 'openai';
    }
    // 默认 OpenAI
    return 'openai';
  }

  /**
   * 根据请求检测客户端协议
   * @param {object} req - Express request 对象
   * @returns {'openai' | 'claude'}
   */
  static detectFromRequest(req) {
    // 优先检查 header
    const formatHeader = req.headers['x-api-format'];
    if (formatHeader === 'claude' || formatHeader === 'anthropic') {
      return 'claude';
    }
    if (formatHeader === 'openai') {
      return 'openai';
    }

    // 其次检查路径
    return this.detectClientProtocol(req.path);
  }

  /**
   * 列出支持的转换方向
   */
  static listSupportedConversions() {
    return {
      request: [
        'openai->gemini',
        'claude->gemini',
        'openai->claude',
        'claude->openai'
      ],
      response: [
        'gemini->openai',
        'gemini->claude',
        'claude->openai',
        'openai->claude'
      ]
    };
  }
}

export default Bridge;
