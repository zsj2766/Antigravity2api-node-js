/**
 * Pipeline Context - 请求链路追踪
 *
 * 用于记录请求在转换链路中各阶段的输入输出，
 * 便于调试转换错误和性能问题。
 *
 * @module utils/pipelineContext
 */

// 最大数据截断长度（增加到 500KB 以容纳 base64 图片）
const MAX_DATA_LENGTH = 500000;

/**
 * 请求链路追踪上下文
 */
export class PipelineContext {
  /**
   * @param {string} requestId - 请求 ID
   */
  constructor(requestId) {
    this.requestId = requestId;
    this.stages = [];
    this.errors = [];
    this.startTime = Date.now();
    this.metadata = {};
  }

  /**
   * 记录转换阶段
   *
   * @param {string} name - 阶段名称
   * @param {Object} input - 输入数据
   * @param {Object} output - 输出数据
   * @param {Object} [metadata] - 额外元数据
   */
  addStage(name, input, output, metadata = {}) {
    const now = Date.now();
    const lastStage = this.stages[this.stages.length - 1];
    const durationMs = lastStage ? now - lastStage.timestamp : now - this.startTime;

    this.stages.push({
      name,
      timestamp: now,
      durationMs,
      input: this.sanitize(input),
      output: this.sanitize(output),
      ...metadata
    });
  }

  /**
   * 记录转换错误
   *
   * @param {string} stage - 阶段名称
   * @param {Error} error - 错误对象
   * @param {Object} [input] - 相关输入数据
   */
  addError(stage, error, input = null) {
    this.errors.push({
      stage,
      timestamp: Date.now(),
      error: error?.message || String(error),
      code: error?.code,
      stack: error?.stack?.split('\n').slice(0, 5).join('\n'),
      input: input ? this.sanitize(input) : null
    });
  }

  /**
   * 设置元数据
   *
   * @param {string} key - 键
   * @param {any} value - 值
   */
  setMetadata(key, value) {
    this.metadata[key] = value;
  }

  /**
   * 数据脱敏处理
   * - 截断 base64 数据
   * - 限制字符串长度
   * - 隐藏敏感字段
   *
   * @param {any} data - 原始数据
   * @returns {any} 脱敏后的数据
   */
  sanitize(data) {
    if (data === null || data === undefined) {
      return null;
    }

    if (typeof data === 'string') {
      return this.sanitizeString(data);
    }

    if (Array.isArray(data)) {
      return data.map(item => this.sanitize(item));
    }

    if (typeof data === 'object') {
      return this.sanitizeObject(data);
    }

    return data;
  }

  /**
   * 字符串处理（保留原始数据，仅截断超长字符串）
   */
  sanitizeString(str) {
    if (str.length <= MAX_DATA_LENGTH) {
      return str;
    }
    return str.slice(0, MAX_DATA_LENGTH) + `...[TRUNCATED:${str.length} chars]`;
  }

  /**
   * 对象处理（隐藏敏感字段，保留 base64 数据）
   */
  sanitizeObject(obj) {
    const result = {};
    const sensitiveKeys = ['password', 'token', 'secret', 'key', 'authorization', 'cookie'];

    for (const [key, value] of Object.entries(obj)) {
      // 隐藏敏感字段
      if (sensitiveKeys.some(k => key.toLowerCase().includes(k))) {
        result[key] = '[REDACTED]';
        continue;
      }

      // 递归处理（不再特殊处理 base64 data 字段）
      result[key] = this.sanitize(value);
    }

    return result;
  }

  /**
   * 检查是否为 base64 字符串（保留用于其他用途）
   */
  isBase64(str) {
    if (typeof str !== 'string' || str.length < 100) return false;
    // 简单检查：长字符串且只包含 base64 字符
    return /^[A-Za-z0-9+/=]+$/.test(str.slice(0, 100));
  }

  /**
   * 获取总耗时
   */
  getTotalDuration() {
    return Date.now() - this.startTime;
  }

  /**
   * 是否有错误
   */
  hasErrors() {
    return this.errors.length > 0;
  }

  /**
   * 获取阶段摘要（用于控制台输出）
   */
  getSummary() {
    const lines = [`[Pipeline: ${this.requestId?.slice(0, 8) || 'unknown'}] ${this.stages.length} stages, ${this.getTotalDuration()}ms`];

    this.stages.forEach((stage, index) => {
      const isLast = index === this.stages.length - 1;
      const prefix = isLast ? '└─' : '├─';
      const hasError = this.errors.some(e => e.stage === stage.name);
      const status = hasError ? '✗' : '✓';
      lines.push(`${prefix} [${index + 1}] ${stage.name} (${stage.durationMs}ms) ${status}`);

      // 如果该阶段有错误，显示错误信息
      if (hasError) {
        const error = this.errors.find(e => e.stage === stage.name);
        lines.push(`│      Error: ${error.error}`);
      }
    });

    return lines.join('\n');
  }

  /**
   * 导出为日志条目格式
   */
  toLogEntry() {
    return {
      stages: this.stages,
      errors: this.errors,
      totalDurationMs: this.getTotalDuration(),
      metadata: this.metadata
    };
  }

  /**
   * 创建子上下文（用于嵌套追踪）
   *
   * @param {string} name - 子上下文名称
   * @returns {PipelineContext}
   */
  createChild(name) {
    const child = new PipelineContext(`${this.requestId}:${name}`);
    child.parentContext = this;
    return child;
  }

  /**
   * 合并子上下文
   *
   * @param {PipelineContext} child - 子上下文
   * @param {string} asStage - 作为阶段名称
   */
  mergeChild(child, asStage) {
    if (!child) return;

    this.addStage(asStage, null, null, {
      childStages: child.stages.length,
      childErrors: child.errors.length,
      childDurationMs: child.getTotalDuration()
    });

    // 合并错误
    this.errors.push(...child.errors.map(e => ({
      ...e,
      stage: `${asStage}:${e.stage}`
    })));
  }
}

/**
 * 创建 Pipeline Context
 *
 * @param {string} requestId - 请求 ID
 * @returns {PipelineContext}
 */
export function createPipelineContext(requestId) {
  return new PipelineContext(requestId);
}

/**
 * 空操作 Pipeline Context（用于日志关闭时）
 */
export const noopPipelineContext = {
  addStage: () => {},
  addError: () => {},
  setMetadata: () => {},
  getTotalDuration: () => 0,
  hasErrors: () => false,
  getSummary: () => '',
  toLogEntry: () => null,
  createChild: () => noopPipelineContext,
  mergeChild: () => {}
};

export default {
  PipelineContext,
  createPipelineContext,
  noopPipelineContext
};
