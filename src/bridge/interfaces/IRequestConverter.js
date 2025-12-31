/**
 * 请求转换接口
 *
 * 定义从一种 API 格式转换到另一种 API 格式的标准接口
 * 子类命名格式：{Source}To{Target}RequestConverter
 */

export class IRequestConverter {
  /**
   * 主入口：转换完整请求体
   * @param {object} body - 源格式请求体
   * @param {object} context - 上下文（config, token 等）
   * @returns {Promise<object>} - 目标格式请求体
   */
  async convert(body, context = {}) {
    throw new Error('convert() must be implemented by subclass');
  }

  /**
   * 转换文本内容
   * @param {string|object} content - 文本内容
   * @returns {object} - 目标格式文本
   */
  convertText(content) {
    throw new Error('convertText() must be implemented by subclass');
  }

  /**
   * 转换图片内容
   * @param {object} content - 图片内容（base64 或 URL）
   * @returns {object} - 目标格式图片
   */
  convertImage(content) {
    throw new Error('convertImage() must be implemented by subclass');
  }

  /**
   * 转换文档内容（PDF 等）
   * @param {object} content - 文档内容
   * @returns {object} - 目标格式文档
   */
  convertDocument(content) {
    throw new Error('convertDocument() must be implemented by subclass');
  }

  /**
   * 转换音频内容（预留）
   * @param {object} content - 音频内容
   * @returns {object} - 目标格式音频
   */
  convertAudio(content) {
    // 预留接口，默认返回空
    return null;
  }

  /**
   * 转换视频内容（预留）
   * @param {object} content - 视频内容
   * @returns {object} - 目标格式视频
   */
  convertVideo(content) {
    // 预留接口，默认返回空
    return null;
  }

  /**
   * 转换工具定义
   * @param {Array} tools - 源格式工具定义数组
   * @returns {Array} - 目标格式工具定义数组
   */
  convertTools(tools) {
    throw new Error('convertTools() must be implemented by subclass');
  }

  /**
   * 转换工具选择配置
   * @param {string|object} toolChoice - 源格式 tool_choice
   * @param {Array} tools - 工具列表（用于特殊映射）
   * @returns {object} - 目标格式工具选择配置
   */
  convertToolConfig(toolChoice, tools) {
    throw new Error('convertToolConfig() must be implemented by subclass');
  }

  /**
   * 转换工具调用结果
   * @param {object} result - 源格式工具结果
   * @returns {object} - 目标格式工具结果
   */
  convertToolResult(result) {
    throw new Error('convertToolResult() must be implemented by subclass');
  }
}

export default IRequestConverter;
