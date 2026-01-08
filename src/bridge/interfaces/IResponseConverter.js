/**
 * 响应转换接口
 *
 * 定义从一种 API 响应格式转换到另一种格式的标准接口
 * 子类命名格式：{Source}To{Target}ResponseConverter
 */

export class IResponseConverter {
  /**
   * 非流式响应转换
   * @param {object} response - 源格式响应
   * @param {object} context - 上下文
   * @returns {object} - 目标格式响应
   */
  convert(response, context = {}) {
    throw new Error('convert() must be implemented by subclass');
  }

  /**
   * 创建流式响应处理器
   *
   * 返回一个有状态的处理器对象，用于处理 SSE 流
   *
   * @param {object} res - Express response 对象
   * @param {object} context - 上下文（requestId, model 等）
   * @returns {object} - 流处理器，包含 process(chunk) 和 finish() 方法
   */
  createStreamProcessor(res, context = {}) {
    throw new Error('createStreamProcessor() must be implemented by subclass');
  }

  /**
   * 转换响应内容（源格式 → 目标格式内容块）
   * @param {Array} parts - 源格式内容块数组
   * @returns {object} - 目标格式内容
   */
  convertContent(parts) {
    throw new Error('convertContent() must be implemented by subclass');
  }

  /**
   * 转换 token 使用统计
   * @param {object} usage - 源格式 usage
   * @returns {object} - 目标格式 usage
   */
  convertUsage(usage) {
    throw new Error('convertUsage() must be implemented by subclass');
  }

  /**
   * 转换错误响应
   * @param {Error|object} error - 源格式错误
   * @returns {object} - 目标格式错误响应
   */
  convertError(error) {
    throw new Error('convertError() must be implemented by subclass');
  }

  /**
   * 构建空响应（无候选结果时）
   * @param {string} requestId - 请求 ID
   * @param {string} model - 模型名称
   * @returns {object} - 目标格式空响应
   */
  buildEmptyResponse(requestId, model) {
    throw new Error('buildEmptyResponse() must be implemented by subclass');
  }
}

export default IResponseConverter;
