/**
 * HTTP 工具模块 (HTTP Utilities)
 *
 * 职责：
 * - 提供 HTTP 请求/响应相关的通用工具函数
 * - 处理敏感信息脱敏
 * - 创建请求快照用于日志记录
 * - SSE (Server-Sent Events) 流式响应辅助
 *
 * 设计说明：
 * - 所有函数均为纯函数，无副作用
 * - 与业务逻辑解耦，可在任意 Controller 中复用
 *
 * @module utils/httpUtils
 */

/**
 * 敏感 HTTP 头列表
 *
 * 这些头部字段包含敏感信息，在日志中应被脱敏处理。
 *
 * @constant {string[]}
 */
const SENSITIVE_HEADERS = ['authorization', 'cookie', 'x-api-key', 'api-key'];

/**
 * 清理请求头中的敏感信息
 *
 * 遍历请求头，将敏感字段的值替换为 '[REDACTED]'。
 * 用于日志记录时保护用户凭证。
 *
 * @param {Object} headers - 原始请求头对象
 * @returns {Object} 脱敏后的请求头对象
 *
 * @example
 * const safeHeaders = sanitizeHeaders(req.headers);
 * // { authorization: '[REDACTED]', 'content-type': 'application/json' }
 */
export function sanitizeHeaders(headers = {}) {
  const result = {};
  Object.entries(headers || {}).forEach(([key, value]) => {
    const lowerKey = String(key).toLowerCase();
    result[key] = SENSITIVE_HEADERS.includes(lowerKey) ? '[REDACTED]' : value;
  });
  return result;
}

/**
 * 创建请求快照
 *
 * 从 Express Request 对象中提取关键信息，
 * 用于日志记录和调试追踪。敏感头部会被自动脱敏。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {Object} 请求快照
 * @property {string} path - 请求路径（含查询字符串）
 * @property {string} method - HTTP 方法
 * @property {Object} headers - 脱敏后的请求头
 * @property {Object} query - URL 查询参数
 * @property {Object} body - 请求体
 *
 * @example
 * const snapshot = createRequestSnapshot(req);
 * logger.info('Request received', snapshot);
 */
export function createRequestSnapshot(req) {
  return {
    path: req.originalUrl,
    method: req.method,
    headers: sanitizeHeaders(req.headers),
    query: req.query,
    body: req.body
  };
}

/**
 * 汇总流式事件内容
 *
 * 将 SSE 流中的多个事件合并为单个响应摘要。
 * 用于日志记录完整的 AI 响应内容。
 *
 * @param {Array<Object>} events - 流式事件数组
 * @returns {Object} 汇总结果
 * @property {string} text - 合并后的文本内容
 * @property {Array|null} tool_calls - 工具调用（如有）
 * @property {string} thinking - 思考过程内容
 *
 * @example
 * const summary = summarizeStreamEvents(streamEvents);
 * // { text: 'Hello, world!', tool_calls: null, thinking: '' }
 */
export function summarizeStreamEvents(events = []) {
  const summary = { text: '', tool_calls: null, thinking: '' };

  events.forEach(event => {
    if (event?.type === 'tool_calls') {
      summary.tool_calls = event.tool_calls;
    } else if (event?.type === 'thinking') {
      summary.thinking += event.content || '';
    } else if (event?.content) {
      summary.text += event.content;
    }
  });

  return summary;
}

/**
 * 提取客户端真实 IP 地址
 *
 * 按优先级检查多个来源：
 * 1. X-Forwarded-For（代理/负载均衡）
 * 2. X-Real-IP（Nginx）
 * 3. 连接地址
 *
 * @param {import('express').Request} req - Express 请求对象
 * @returns {string} 客户端 IP 地址，无法获取时返回 'unknown'
 *
 * @example
 * const clientIP = extractClientIP(req);
 * logger.info(`Request from ${clientIP}`);
 */
export function extractClientIP(req) {
  return (
    req.headers['x-forwarded-for']?.split(',')[0]?.trim() ||
    req.headers['x-real-ip'] ||
    req.connection?.remoteAddress ||
    req.socket?.remoteAddress ||
    req.ip ||
    'unknown'
  );
}

/**
 * 设置 SSE 响应头
 *
 * ���置 Server-Sent Events 所需的 HTTP 响应头，
 * 并启动心跳机制防止连接超时。
 *
 * @param {import('express').Response} res - Express 响应对象
 * @param {Object} options - 配置选项
 * @param {number} [options.heartbeatInterval=15000] - 心跳间隔（毫秒）
 *
 * @example
 * setStreamHeaders(res);
 * res.write('data: {"message": "hello"}\n\n');
 */
export function setStreamHeaders(res, options = {}) {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');

  // 初始化 locals
  if (!res.locals) res.locals = {};
  if (res.locals.streamBodySent === undefined) {
    res.locals.streamBodySent = false;
  }

  // 心跳机制：定期发送 SSE 注释保持连接
  const interval = options.heartbeatInterval || 15000;

  if (!res.locals.heartbeatTimer) {
    res.locals.heartbeatTimer = setInterval(() => {
      if (!res.writableEnded && res.headersSent) {
        res.write(': keep-alive\n\n');
      }
    }, interval);

    // 请求关闭时清理定时器
    res.on('close', () => {
      if (res.locals?.heartbeatTimer) {
        clearInterval(res.locals.heartbeatTimer);
        res.locals.heartbeatTimer = null;
      }
    });
  }
}

/**
 * 清理 SSE 心跳定时器
 *
 * 在结束 SSE 流之前调用，确保资源正确释放。
 *
 * @param {import('express').Response} res - Express 响应对象
 */
export function clearHeartbeat(res) {
  if (res.locals?.heartbeatTimer) {
    clearInterval(res.locals.heartbeatTimer);
    res.locals.heartbeatTimer = null;
  }
}

/**
 * 创建 OpenAI 兼容的响应元数据
 *
 * 生成用于 Chat Completions API 响应的 ID 和时间戳。
 *
 * @returns {Object} 响应元数据
 * @property {string} id - 响应 ID（格式：chatcmpl-{timestamp}）
 * @property {number} created - Unix 时间戳（秒）
 *
 * @example
 * const { id, created } = createResponseMeta();
 * // { id: 'chatcmpl-1703836800000', created: 1703836800 }
 */
export function createResponseMeta() {
  return {
    id: `chatcmpl-${Date.now()}`,
    created: Math.floor(Date.now() / 1000)
  };
}

/**
 * 创建 SSE 流式响应数据块
 *
 * 构建符合 OpenAI Chat Completions API 流式响应格式的数据对象。
 *
 * @param {string} id - 响应 ID
 * @param {number} created - 创建时间戳（秒）
 * @param {string} model - 模型名称
 * @param {Object} delta - 增量内容
 * @param {string|null} [finishReason=null] - 结束原因
 * @param {Object|null} [usage=null] - Token 使用统计
 * @returns {Object} SSE 数据块
 *
 * @example
 * const chunk = createStreamChunk(id, created, 'gpt-4', { content: 'Hello' });
 * res.write(`data: ${JSON.stringify(chunk)}\n\n`);
 */
export function createStreamChunk(id, created, model, delta, finishReason = null, usage = null) {
  return {
    id,
    object: 'chat.completion.chunk',
    created,
    model,
    choices: [{ index: 0, delta, finish_reason: finishReason }],
    ...(usage ? { usage } : {})
  };
}

/**
 * 写入 SSE 数据
 *
 * 将数据对象序列化为 SSE 格式并写入响应流。
 *
 * @param {import('express').Response} res - Express 响应对象
 * @param {Object} data - 要发送的数据
 *
 * @example
 * writeStreamData(res, { id: 'msg_1', content: 'Hello' });
 * // 输出: data: {"id":"msg_1","content":"Hello"}\n\n
 */
export function writeStreamData(res, data) {
  if (res.locals) {
    res.locals.streamBodySent = true;
  }
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

/**
 * 结束 SSE 流
 *
 * 发送最终数据块、[DONE] 标记，并关闭响应。
 * 自动清理心跳定时器。
 *
 * @param {import('express').Response} res - Express 响应对象
 * @param {string} id - 响应 ID
 * @param {number} created - 创建时间戳
 * @param {string} model - 模型名称
 * @param {string} finishReason - 结束原因（如 'stop', 'length'）
 * @param {Object|null} [usage=null] - Token 使用统计
 *
 * @example
 * endStream(res, id, created, 'gpt-4', 'stop', { total_tokens: 100 });
 */
export function endStream(res, id, created, model, finishReason, usage = null) {
  clearHeartbeat(res);
  writeStreamData(res, createStreamChunk(id, created, model, {}, finishReason, usage));
  res.write('data: [DONE]\n\n');
  res.end();
}

/**
 * 计算重试延迟时间
 *
 * 根据重试次数和错误信息计算下次重试的等待时间。
 * 优先使用服务器返回的 Retry-After 头，否则使用指数退避策略。
 *
 * @param {number} attempt - 当前重试次数（从 0 开始）
 * @param {Error|Object} error - 错误对象，可能包含 retryAfter 或 response.headers
 * @returns {number} 延迟时间（毫秒）
 *
 * @example
 * const delay = calculateRetryDelay(1, error);
 * await new Promise(resolve => setTimeout(resolve, delay));
 */
export function calculateRetryDelay(attempt, error) {
  const initialDelay = 1000;
  const maxDelay = 10000;

  // 1. 检查错误对象中的 retryAfter（已由 client.js 解析）
  if (error?.retryAfter && typeof error.retryAfter === 'number') {
    return error.retryAfter; // 已经是毫秒
  }

  // 2. 检查 Retry-After 响应头
  const retryAfter = error?.response?.headers?.['retry-after'] || error?.headers?.['retry-after'];
  if (retryAfter) {
    const delay = parseInt(retryAfter, 10);
    if (!isNaN(delay)) return delay * 1000; // 秒转毫秒
  }

  // 3. 指数退避 + 随机抖动
  const backoff = Math.min(initialDelay * Math.pow(2, attempt), maxDelay);
  const jitter = Math.random() * 1000;
  return backoff + jitter;
}

export { SENSITIVE_HEADERS };
