import axios from 'axios';
import http from 'http';
import https from 'https';
import tokenManager from '../auth/token_manager.js';
import config from '../config/config.js';
import { log } from '../utils/logger.js';
import { generateRequestId } from '../utils/idGenerator.js';
import AntigravityRequester from '../AntigravityRequester.js';
import { registerTextThoughtSignature, registerThoughtSignature } from '../utils/utils.js';
import { GeminiToOpenAIResponseConverter } from '../bridge/res/GeminiToOpenAIResponseConverter.js';
import { GeminiToClaudeResponseConverter } from '../bridge/res/GeminiToClaudeResponseConverter.js';
import { CallbackProtocolEmitter } from '../bridge/common/CallbackProtocolEmitter.js';
import { mapGeminiStopReason, generateToolCallId } from '../bridge/common/index.js';
import { saveBase64Image } from '../utils/imageStorage.js';

// 创建转换器实例
const geminiToOpenAIConverter = new GeminiToOpenAIResponseConverter();
const geminiToClaudeConverter = new GeminiToClaudeResponseConverter();

/**
 * 转换 Gemini usage 到 OpenAI 格式
 */
function toOpenAiUsage(usageMetadata) {
  return geminiToOpenAIConverter.convertUsage(usageMetadata);
}

/**
 * 转换工具调用并附加签名
 */
function convertToToolCallWithSignature(functionCall, thoughtSignature) {
  const id = functionCall.id || generateToolCallId();
  if (thoughtSignature) {
    registerThoughtSignature(id, thoughtSignature);
  }
  return {
    id,
    type: 'function',
    function: {
      name: functionCall.name,
      arguments: JSON.stringify(functionCall.args || {})
    }
  };
}

/**
 * 解析 Gemini 流式响应到 OpenAI 格式
 * 处理 SSE 格式的行，解析 JSON 并调用回调
 * @param {string} line - SSE 行
 * @param {Object} state - 状态对象 { toolCalls, toolCallIndex, usage, textAccumulator, finishReason }
 * @param {Function} callback - 回调函数，接收事件对象
 */
export function parseGeminiStreamToOpenAI(line, state, callback) {
  // 跳过空行和非数据行
  if (!line || !line.startsWith('data: ')) return;

  const jsonStr = line.slice(6).trim();
  if (!jsonStr || jsonStr === '[DONE]') return;

  try {
    const chunk = JSON.parse(jsonStr);
    const candidate = chunk.candidates?.[0];
    if (!candidate) return;

    const parts = candidate.content?.parts || [];

    for (const part of parts) {
      if (!part) continue;

      // 思考内容
      if (part.thought === true) {
        if (part.text) {
          if (part.thoughtSignature) {
            registerTextThoughtSignature(part.text, part.thoughtSignature);
          }
          callback({ type: 'thinking', content: part.text, signature: part.thoughtSignature || null });
        } else if (part.thoughtSignature) {
          // 单独的 signature chunk（没有 text）
          callback({ type: 'thinking', content: null, signature: part.thoughtSignature });
        }
        continue;
      }

      // 普通文本
      if (part.text !== undefined) {
        if (part.thoughtSignature) {
          registerTextThoughtSignature(part.text, part.thoughtSignature);
        }
        callback({ type: 'text', content: part.text });
      }

      // 工具调用
      if (part.functionCall) {
        const toolCall = convertToToolCallWithSignature(part.functionCall, part.thoughtSignature);
        state.toolCalls.push(toolCall);
        callback({ type: 'tool_call_chunk', tool_call: toolCall });
      }

      // 图片数据
      if (part.inlineData) {
        const imageUrl = saveBase64Image(part.inlineData.data, part.inlineData.mimeType);
        callback({ type: 'image', url: imageUrl });
      }

      // 文件数据
      if (part.fileData) {
        callback({ type: 'file', url: part.fileData.fileUri, mimeType: part.fileData.mimeType });
      }
    }

    // 处理 usage
    if (chunk.usageMetadata) {
      state.usage = toOpenAiUsage(chunk.usageMetadata);
    }

    // 处理 finish reason - 返回原始 Gemini finishReason，由调用方根据目标格式映射
    if (candidate.finishReason) {
      callback({ type: 'finish_reason', finishReason: candidate.finishReason });
    }
  } catch (e) {
    // 解析错误静默忽略
  }
}

// HTTP Keep-Alive Agent 配置（复用 TCP 连接提升性能）
const agentOptions = {
    keepAlive: true,
    keepAliveMsecs: 1000,
    maxSockets: 100,
    maxFreeSockets: 10,
    timeout: 60000
};
const httpAgent = new http.Agent(agentOptions);
const httpsAgent = new https.Agent(agentOptions);

// 静态 Headers（避免每次请求重复创建）
const STATIC_HEADERS = {
    'Content-Type': 'application/json',
    'Accept-Encoding': 'gzip'
};

// 请求客户端：优先使用 AntigravityRequester，失败则降级到 axios
let requester = null;
let useAxios = false;
const REQUESTER_FALLBACK_ERROR_KEYWORDS = ['upstream error', 'do request failed', 'process closed'];

if (config.useNativeAxios === true) {
    useAxios = true;
} else {
    try {
        requester = new AntigravityRequester();
    } catch (error) {
        console.warn('AntigravityRequester 初始化失败，降级使用 axios:', error.message);
        useAxios = true;
    }
}

export function refreshApiClientConfig() {
    if (config.useNativeAxios === true) {
        requester = null;
        useAxios = true;
        return;
    }

    if (config.useNativeAxios === false) {
        useAxios = false;
    }

    if (!requester && !useAxios) {
        try {
            requester = new AntigravityRequester();
        } catch (error) {
            console.warn('重新初始化 AntigravityRequester 失败，继续使用 axios:', error.message);
            useAxios = true;
        }
    }
}

// ==================== 辅助函数 ====================

function buildHeaders(token) {
    return {
        ...STATIC_HEADERS,
        'Host': config.api.host,
        'User-Agent': config.api.userAgent,
        'Authorization': `Bearer ${token.access_token}`
    };
}

function buildAxiosConfig(url, headers, body = null) {
    const axiosConfig = {
        method: 'POST',
        url,
        headers,
        timeout: config.timeout
    };

    // 仅在无 proxy 时使用 Keep-Alive Agent（proxy 与自定义 Agent 存在兼容性问题）
    if (config.proxy) {
        const proxyUrl = new URL(config.proxy);
        axiosConfig.proxy = {
            protocol: proxyUrl.protocol.replace(':', ''),
            host: proxyUrl.hostname,
            port: parseInt(proxyUrl.port)
        };
    } else {
        axiosConfig.httpAgent = httpAgent;
        axiosConfig.httpsAgent = httpsAgent;
    }

    if (body !== null) axiosConfig.data = body;
    return axiosConfig;
}

// ==================== 额度相关函数 ====================

export async function getModelsWithQuotas(token) {
    const headers = buildHeaders(token);

    try {
        let data;
        if (useAxios) {
            data = (await axios(buildAxiosConfig(config.api.modelsUrl, headers, {}))).data;
        } else {
            const response = await requester.antigravity_fetch(config.api.modelsUrl, buildRequesterConfig(headers, {}));
            if (response.status !== 200) {
                const errorBody = await response.text();
                throw { status: response.status, message: errorBody };
            }
            data = await response.json();
        }

        const quotas = {};
        Object.entries(data.models || {}).forEach(([modelId, modelData]) => {
            if (modelData.quotaInfo) {
                quotas[modelId] = {
                    remaining: modelData.quotaInfo.remainingFraction || modelData.quotaInfo.remaining || 0,
                    resetTime: modelData.quotaInfo.resetTime || null,
                    resetTimeRaw: modelData.quotaInfo.resetTime
                };
            }
        });

        return quotas;
    } catch (error) {
        await handleApiError(error, token);
    }
}

function buildRequesterConfig(headers, body = null) {
    const reqConfig = {
        method: 'POST',
        headers,
        timeout_ms: config.timeout,
        proxy: config.proxy
    };
    if (body !== null) reqConfig.body = JSON.stringify(body);
    return reqConfig;
}

function shouldFallbackToAxios(error) {
    if (useAxios || !error) return false;

    const message = String(error?.message || '').toLowerCase();
    return REQUESTER_FALLBACK_ERROR_KEYWORDS.some(keyword => message.includes(keyword));
}

async function withRequesterFallback(fn) {
    try {
        return await fn(useAxios);
    } catch (error) {
        if (shouldFallbackToAxios(error)) {
            console.warn('AntigravityRequester 调用失败，降级使用 axios:', error.message);
            useAxios = true;
            return await fn(useAxios);
        }

        throw error;
    }
}

function statusFromStatusText(statusText) {
    if (!statusText) return null;

    const normalized = String(statusText).toUpperCase();
    if (normalized === 'RESOURCE_EXHAUSTED') return 429;
    if (normalized === 'INTERNAL') return 500;
    if (normalized === 'UNAUTHENTICATED') return 401;

    const numeric = parseInt(statusText, 10);
    return Number.isNaN(numeric) ? null : numeric;
}

function extractErrorInfo(errorObj) {
    const details = Array.isArray(errorObj?.details) ? errorObj.details : [];
    const errorInfo = details.find(
        detail => typeof detail === 'object' && detail['@type']?.includes('ErrorInfo')
    );
    if (!errorInfo) return null;
    return {
        reason: errorInfo.reason,
        domain: errorInfo.domain,
        metadata: errorInfo.metadata
    };
}

function parseRetryDelayMs(errorInfo, message) {
    let retryDelayMs = null;

    const retryDetail = errorInfo?.details?.find(
        detail => typeof detail === 'object' && detail['@type']?.includes('RetryInfo')
    );

    if (retryDetail?.retryDelay) {
        const secondsMatch = /([0-9]+(?:\.[0-9]+)?)s/.exec(retryDetail.retryDelay);
        if (secondsMatch) {
            retryDelayMs = Math.ceil(parseFloat(secondsMatch[1]) * 1000);
        }
    }

    if (!retryDelayMs && typeof message === 'string') {
        const messageMatch = /retry in ([0-9]+(?:\.[0-9]+)?)s/i.exec(message);
        if (messageMatch) {
            retryDelayMs = Math.ceil(parseFloat(messageMatch[1]) * 1000);
        }
    }

    return retryDelayMs;
}

function detectEmbeddedError(body) {
    if (!body) return null;

    try {
        const parsed = typeof body === 'string' ? JSON.parse(body) : body;

        // 支持两种格式：
        // 1. { "error": { "code": 429, ... } } - 标准格式
        // 2. { "code": 429, "status": "RESOURCE_EXHAUSTED", ... } - 直接格式
        let errorObj = null;

        if (parsed?.error) {
            errorObj = parsed.error;
        } else if (parsed?.code || parsed?.status) {
            // 直接格式：{ "code": 429, "status": "RESOURCE_EXHAUSTED", "message": "..." }
            errorObj = parsed;
        }

        if (!errorObj) return null;

        const errorInfo = extractErrorInfo(errorObj);
        const messageText = typeof errorObj.message === 'string' ? errorObj.message : body;
        const capacityExhausted = errorInfo?.reason === 'MODEL_CAPACITY_EXHAUSTED' ||
            /no capacity available|capacity exhausted/i.test(String(messageText));
        const status = capacityExhausted ? 503 : statusFromStatusText(errorObj.code || errorObj.status);
        const retryDelayMs = parseRetryDelayMs(errorObj, errorObj.message || body);

        return {
            status,
            message: JSON.stringify(errorObj, null, 2),
            retryDelayMs,
            disableToken: status === 401,
            reason: errorInfo?.reason,
            capacityExhausted
        };
    } catch (e) {
        return null;
    }
}

async function extractErrorDetails(error) {
    let status = statusFromStatusText(error?.status || error?.statusCode || error?.response?.status);
    let message = error?.message || error?.response?.statusText || 'Unknown error';
    let retryDelayMs = error?.retryDelayMs || null;
    let disableToken = error?.disableToken === true;
    let rawResponse = null;
    let errorReason = null;
    let capacityExhausted = false;

    if (error?.response?.data?.readable) {
        const chunks = [];
        for await (const chunk of error.response.data) {
            chunks.push(chunk);
        }
        rawResponse = Buffer.concat(chunks).toString();
        message = rawResponse;
    } else if (typeof error?.response?.data === 'object') {
        rawResponse = error.response.data;
        message = JSON.stringify(error.response.data, null, 2);
    } else if (error?.response?.data) {
        rawResponse = error.response.data;
        message = error.response.data;
    } else if (error?.message && error?.message !== message) {
        message = error.message;
    }

    const embeddedError = detectEmbeddedError(message);
    if (embeddedError) {
        status = embeddedError.status ?? status;
        retryDelayMs = embeddedError.retryDelayMs ?? retryDelayMs;
        disableToken = embeddedError.disableToken || disableToken;
        message = embeddedError.message;
        errorReason = embeddedError.reason ?? errorReason;
        capacityExhausted = embeddedError.capacityExhausted || capacityExhausted;
    } else if (typeof message === 'string') {
        const normalizedMessage = message.toLowerCase();
        if (normalizedMessage.includes('no capacity available') || normalizedMessage.includes('capacity exhausted')) {
            capacityExhausted = true;
        }
    }

    return {
        status: status ?? 'Unknown',
        message,
        retryDelayMs,
        disableToken,
        rawResponse,
        errorReason,
        capacityExhausted
    };
}

// 统一错误处理
async function handleApiError(error, token) {
    const details = await extractErrorDetails(error);

    if (details.status === 403 || details.status === 401 || details.disableToken) {
        tokenManager.disableCurrentToken(token);
        const err = new Error(`该账号没有使用权限或凭证失效，已自动禁用。错误详情: ${details.message}`);
        err.status = details.status;
        err.code = 'TOKEN_DISABLED';
        err.rawResponse = details.rawResponse;
        throw err;
    }

    if (details.capacityExhausted) {
        const err = new Error(`模型暂无容量，请稍后重试。错误详情: ${details.message}`);
        err.status = 503;
        err.code = 'CAPACITY_EXHAUSTED';
        err.retryAfter = details.retryDelayMs;
        err.rawResponse = details.rawResponse;
        throw err;
    }

    const err = new Error(`API请求失败 (${details.status}): ${details.message}`);
    err.status = details.status;
    err.retryAfter = details.retryDelayMs;  // 暴露重试延迟（毫秒）
    err.code = details.status === 429 ? 'RATE_LIMITED' : 'API_ERROR';
    err.rawResponse = details.rawResponse;
    throw err;
}

// ==================== 导出函数 ====================

/**
 * 流式生成响应（回调模式）- 仅用于图片生成模型
 *
 * 图片生成需要特殊的聚合逻辑（收集所有图片 URL 后统一输出），
 * 因此使用回调模式而非原始流模式。
 *
 * @param {Object} requestBody - 请求体
 * @param {Object} token - 认证 token
 * @param {Function} callback - 回调函数，接收解析后的事件对象 { type, content, ... }
 */
export async function generateImageModelResponse(requestBody, token, callback) {

    const state = { toolCalls: [], toolCallIndex: 0, usage: null, textAccumulator: { text: '', signature: null }, finishReason: null };
    let buffer = ''; // 缓冲区：处理跨 chunk 的不完整行
    let streamChunks = []; // 收集流式响应（用于 debug=high 日志）

    const processChunk = (chunk) => {
        buffer += chunk;
        streamChunks.push(chunk); // 收集响应片段
        const lines = buffer.split('\n');
        buffer = lines.pop(); // 保留最后一行（可能不完整）
        lines.forEach(line => parseGeminiStreamToOpenAI(line, state, (data) => {
            // 拦截 finish_reason 事件，存入 state 而非透传给上层
            if (data.type === 'finish_reason') {
                state.finishReason = data.finishReason;
            } else {
                callback(data);
            }
        }));
    };

    try {
        await withRequesterFallback(async currentUseAxios => {
            const headers = buildHeaders(token);
            buffer = ''; // 重置缓冲区
            const attemptStartTime = Date.now();

            // 记录请求
            log.backend({
                type: 'request',
                url: config.api.url,
                method: 'POST',
                headers,
                body: requestBody,
                tokenId: token.projectId || token.access_token?.slice(-8)
            });

            try {
                if (currentUseAxios) {
                    const axiosConfig = { ...buildAxiosConfig(config.api.url, headers, requestBody), responseType: 'stream' };
                    const response = await axios(axiosConfig);

                    response.data.on('data', chunk => processChunk(chunk.toString()));
                    await new Promise((resolve, reject) => {
                        response.data.on('end', resolve);
                        response.data.on('error', reject);
                    });

                    // 记录成功响应
                    log.backend({
                        type: 'response',
                        status: 200,
                        durationMs: Date.now() - attemptStartTime,
                        tokenId: token.projectId || token.access_token?.slice(-8)
                    });
                    return;
                }

                const streamResponse = requester.antigravity_fetchStream(config.api.url, buildRequesterConfig(headers, requestBody));
                let errorBody = '';
                let statusCode = null;

                await new Promise((resolve, reject) => {
                    streamResponse
                        .onStart(({ status }) => { statusCode = status; })
                        .onData((chunk) => statusCode !== 200 ? errorBody += chunk : processChunk(chunk))
                        .onEnd(() => {
                            if (statusCode !== 200) {
                                // 记录失败响应
                                log.backend({
                                    type: 'response',
                                    status: statusCode,
                                    durationMs: Date.now() - attemptStartTime,
                                    body: errorBody,
                                    tokenId: token.projectId || token.access_token?.slice(-8)
                                });
                                reject({ status: statusCode, message: errorBody });
                            } else {
                                // 记录成功响应
                                log.backend({
                                    type: 'response',
                                    status: 200,
                                    durationMs: Date.now() - attemptStartTime,
                                    tokenId: token.projectId || token.access_token?.slice(-8)
                                });
                                resolve();
                            }
                        })
                        .onError((err) => {
                            log.backend({
                                type: 'response',
                                status: 'Error',
                                durationMs: Date.now() - attemptStartTime,
                                body: err?.message || err,
                                tokenId: token.projectId || token.access_token?.slice(-8)
                            });
                            reject(err);
                        });
                });
            } catch (error) {
                // axios 错误也记录
                if (currentUseAxios) {
                    log.backend({
                        type: 'response',
                        status: error?.response?.status || 'Error',
                        durationMs: Date.now() - attemptStartTime,
                        body: error?.message || error,
                        tokenId: token.projectId || token.access_token?.slice(-8)
                    });
                }
                throw error;
            }
        });
    } catch (error) {
        // 统一通过 handleApiError 标准化所有错误（包括 Axios 原生错误）
        await handleApiError(error, token);
    }

    return { usage: state.usage, finishReason: state.finishReason };
}

/**
 * 流式生成响应 - 原始流模式
 * 返回 AsyncIterable<GeminiChunk>，由调用方自行处理
 *
 * @param {Object} requestBody - 请求体
 * @param {Object} token - 认证 token
 * @returns {AsyncGenerator<{chunk: Object, usage: Object|null, finishReason: string|null}>}
 */
export async function* generateAssistantResponseStream(requestBody, token) {
    let buffer = '';
    let usage = null;
    let finishReason = null;

    const parseSSELine = (line) => {
        if (!line || !line.startsWith('data: ')) return null;
        const jsonStr = line.slice(6).trim();
        if (!jsonStr || jsonStr === '[DONE]') return null;
        try {
            return JSON.parse(jsonStr);
        } catch (e) {
            return null;
        }
    };

    // 创建一个队列来收集 chunks
    const chunks = [];
    let resolveNext = null;
    let streamEnded = false;
    let streamError = null;

    const pushChunk = (chunk) => {
        if (resolveNext) {
            resolveNext({ value: chunk, done: false });
            resolveNext = null;
        } else {
            chunks.push(chunk);
        }
    };

    const endStream = (error = null) => {
        streamEnded = true;
        streamError = error;
        if (resolveNext) {
            if (error) {
                resolveNext({ value: undefined, done: true, error });
            } else {
                resolveNext({ value: undefined, done: true });
            }
            resolveNext = null;
        }
    };

    const processChunk = (text) => {
        buffer += text;
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
            const parsed = parseSSELine(line);
            if (!parsed) continue;

            // Gemini 返回格式: { response: { candidates, usageMetadata } } 或直接 { candidates, usageMetadata }
            // 统一解包为标准格式 { candidates, usageMetadata }
            const normalized = parsed.response || parsed;

            // 提取 usage 和 finishReason
            if (normalized.usageMetadata) {
                usage = toOpenAiUsage(normalized.usageMetadata);
            }
            const candidate = normalized.candidates?.[0];
            if (candidate?.finishReason) {
                finishReason = candidate.finishReason;
            }

            // 返回标准化后的 chunk，确保下游 processor 可以直接访问 candidates
            pushChunk({ chunk: normalized, usage, finishReason });
        }
    };

    // 启动流请求（异步）
    const streamPromise = withRequesterFallback(async currentUseAxios => {
        const headers = buildHeaders(token);
        const attemptStartTime = Date.now();
        const tokenId = token.projectId || token.access_token?.slice(-8);

        log.backend({
            type: 'request',
            url: config.api.url,
            method: 'POST',
            headers,
            body: requestBody,
            tokenId
        });

        try {
            if (currentUseAxios) {
                const axiosConfig = { ...buildAxiosConfig(config.api.url, headers, requestBody), responseType: 'stream' };
                const response = await axios(axiosConfig);

                response.data.on('data', chunk => processChunk(chunk.toString()));
                await new Promise((resolve, reject) => {
                    response.data.on('end', () => {
                        log.backend({
                            type: 'response',
                            status: 200,
                            durationMs: Date.now() - attemptStartTime,
                            tokenId
                        });
                        endStream();
                        resolve();
                    });
                    response.data.on('error', (err) => {
                        endStream(err);
                        reject(err);
                    });
                });
                return;
            }

            const streamResponse = requester.antigravity_fetchStream(config.api.url, buildRequesterConfig(headers, requestBody));
            let errorBody = '';
            let statusCode = null;

            await new Promise((resolve, reject) => {
                streamResponse
                    .onStart(({ status }) => { statusCode = status; })
                    .onData((chunk) => {
                        if (statusCode !== 200) {
                            errorBody += chunk;
                        } else {
                            processChunk(chunk);
                        }
                    })
                    .onEnd(() => {
                        if (statusCode !== 200) {
                            log.backend({
                                type: 'response',
                                status: statusCode,
                                durationMs: Date.now() - attemptStartTime,
                                body: errorBody,
                                tokenId
                            });
                            const err = { status: statusCode, message: errorBody };
                            endStream(err);
                            reject(err);
                        } else {
                            log.backend({
                                type: 'response',
                                status: 200,
                                durationMs: Date.now() - attemptStartTime,
                                tokenId
                            });
                            endStream();
                            resolve();
                        }
                    })
                    .onError((err) => {
                        log.backend({
                            type: 'response',
                            status: 'Error',
                            durationMs: Date.now() - attemptStartTime,
                            body: err?.message || err,
                            tokenId
                        });
                        endStream(err);
                        reject(err);
                    });
            });
        } catch (error) {
            if (currentUseAxios) {
                log.backend({
                    type: 'response',
                    status: error?.response?.status || 'Error',
                    durationMs: Date.now() - attemptStartTime,
                    body: error?.message || error,
                    tokenId
                });
            }
            throw error;
        }
    }).catch(async (error) => {
        try {
            await handleApiError(error, token);
        } catch (e) {
            endStream(e);
        }
    });

    // 异步迭代器：等待 chunks 或流结束
    while (true) {
        if (chunks.length > 0) {
            yield chunks.shift();
        } else if (streamEnded) {
            if (streamError) {
                throw streamError;
            }
            break;
        } else {
            // 等待下一个 chunk
            await new Promise(resolve => {
                resolveNext = (result) => {
                    if (result.done) {
                        resolve();
                    } else {
                        chunks.push(result.value);
                        resolve();
                    }
                };
            });
        }
    }

    // 等待流请求完成（处理可能的错误）
    await streamPromise;
}

export async function getAvailableModels() {
    const token = await tokenManager.getToken();
    if (!token) throw new Error('没有可用的token，请运行 npm run login 获取token');

    const data = await withRequesterFallback(async currentUseAxios => {
        const headers = buildHeaders(token);
        const attemptStartTime = Date.now();
        const tokenId = token.projectId || token.access_token?.slice(-8);

        // 记录请求
        log.backend({
            type: 'request',
            url: config.api.modelsUrl,
            method: 'POST',
            headers,
            body: {},
            tokenId
        });

        try {
            let responseData;
            if (currentUseAxios) {
                responseData = (await axios(buildAxiosConfig(config.api.modelsUrl, headers, {}))).data;
            } else {
                const response = await requester.antigravity_fetch(config.api.modelsUrl, buildRequesterConfig(headers, {}));
                const bodyText = await response.text();
                const embeddedError = detectEmbeddedError(bodyText);

                if (response.status !== 200 || embeddedError) {
                    throw {
                        status: embeddedError?.status ?? response.status,
                        message: embeddedError?.message ?? bodyText,
                        retryDelayMs: embeddedError?.retryDelayMs,
                        disableToken: embeddedError?.disableToken
                    };
                }

                responseData = JSON.parse(bodyText);
            }

            // 记录成功响应
            log.backend({
                type: 'response',
                status: 200,
                durationMs: Date.now() - attemptStartTime,
                tokenId
            });

            return responseData;
        } catch (error) {
            // 记录失败响应
            log.backend({
                type: 'response',
                status: error?.status || error?.response?.status || 'Error',
                durationMs: Date.now() - attemptStartTime,
                body: error?.message || error,
                tokenId
            });
            throw error;
        }
    });

    return {
        object: 'list',
        data: Object.keys(data.models).map(id => ({
            id,
            object: 'model',
            created: Math.floor(Date.now() / 1000),
            owned_by: 'google'
        }))
    };
}

// 内部复用的非流式请求封装，返回上游原始 JSON，方便不同上层按需解析
async function callNoStreamApi(requestBody, token) {
    return await withRequesterFallback(async currentUseAxios => {
        const headers = buildHeaders(token);
        const attemptStartTime = Date.now();
        const tokenId = token.projectId || token.access_token?.slice(-8);

        // 记录请求
        log.backend({
            type: 'request',
            url: config.api.noStreamUrl,
            method: 'POST',
            headers,
            body: requestBody,
            tokenId
        });

        try {
            let responseData;
            if (currentUseAxios) {
                responseData = (await axios(buildAxiosConfig(config.api.noStreamUrl, headers, requestBody))).data;
            } else {
                const response = await requester.antigravity_fetch(
                    config.api.noStreamUrl,
                    buildRequesterConfig(headers, requestBody)
                );
                const bodyText = await response.text();
                const embeddedError = detectEmbeddedError(bodyText);

                if (response.status !== 200 || embeddedError) {
                    throw {
                        status: embeddedError?.status ?? response.status,
                        message: embeddedError?.message ?? bodyText,
                        retryDelayMs: embeddedError?.retryDelayMs,
                        disableToken: embeddedError?.disableToken
                    };
                }

                responseData = JSON.parse(bodyText);
            }

            // 记录成功响应
            log.backend({
                type: 'response',
                status: 200,
                durationMs: Date.now() - attemptStartTime,
                tokenId
            });

            return responseData;
        } catch (error) {
            // 记录失败响应
            log.backend({
                type: 'response',
                status: error?.status || error?.response?.status || 'Error',
                durationMs: Date.now() - attemptStartTime,
                body: error?.message || error,
                tokenId
            });
            throw error;
        }
    });
}

export async function generateAssistantResponseNoStream(requestBody, token) {

    let data;
    let aggregatedText = '';
    let aggregatedTextSignature = null;

    try {
        data = await callNoStreamApi(requestBody, token);
    } catch (error) {
        await handleApiError(error, token);
    }

    // 解析响应内容
    const candidate = data.response?.candidates?.[0];
    const parts = candidate?.content?.parts || [];

    // 使用 Converter 保持原始顺序的 Claude 内容块
    let contentBlocks = [];
    try {
        contentBlocks = await geminiToClaudeConverter.convertContentAsync(parts, saveBase64Image);
    } catch {
        contentBlocks = geminiToClaudeConverter.convertContent(parts);
    }
    const usage = toOpenAiUsage(data.response?.usageMetadata);
    let content = '';
    let thinkingContent = '';
    let thinkingSignature = null;
    const toolCalls = [];
    const images = [];  // 独立的图片结构
    const files = [];   // 独立的文件结构

    for (const part of parts) {
        if (part.thought === true) {
            thinkingContent += part.text || '';
            // 保存最后一个 thinking 的 signature
            if (part.thoughtSignature) {
                thinkingSignature = part.thoughtSignature;
            }
        } else if (part.text !== undefined) {
            if (part.thoughtSignature) {
                registerTextThoughtSignature(part.text, part.thoughtSignature);
                aggregatedTextSignature = part.thoughtSignature;
            }
            aggregatedText += part.text || '';
            content += part.text;
        } else if (part.functionCall) {
            toolCalls.push(convertToToolCallWithSignature(part.functionCall, part.thoughtSignature));
        } else if (part.inlineData) {
            // 保存图片到本地并获取 URL，返回独立结构
            const imageUrl = saveBase64Image(part.inlineData.data, part.inlineData.mimeType);
            images.push({
                url: imageUrl,
                mimeType: part.inlineData.mimeType,
                base64: part.inlineData.data
            });
        } else if (part.fileData) {
            files.push({
                url: part.fileData.fileUri,
                mimeType: part.fileData.mimeType
            });
        }
    }

    // 注册聚合文本的签名
    if (aggregatedText && aggregatedTextSignature) {
        registerTextThoughtSignature(aggregatedText, aggregatedTextSignature);
    }

    // 返回原始 Gemini finishReason，由调用方根据目标格式映射
    const rawFinishReason = candidate?.finishReason;
    const hasToolCalls = toolCalls.length > 0;

    // 返回统一结构，包含独立的 images 数组和原始 finishReason
    return {
        content,
        toolCalls,
        usage,
        finishReason: rawFinishReason,  // 原始 Gemini finishReason
        hasToolCalls,                    // 供调用方用于 stop_reason 映射
        thinking: thinkingContent || null,
        thinkingSignature,
        images: images.length > 0 ? images : null,
        files: files.length > 0 ? files : null,
        contentBlocks  // 使用 Converter 保持原始顺序的 Claude 内容块
    };
}

// 直接返回原始 Gemini 风格响应（用于 Gemini 兼容接口）
export async function generateGeminiResponseNoStream(requestBody, token) {
    try {
        const data = await callNoStreamApi(requestBody, token);
        // 上游返回通常为 { response: { ... } } 结构，这里只透传内部 response
        return data?.response ?? data;
    } catch (error) {
        throw error;
    }
}

export function closeRequester() {
    if (requester) requester.close();
}
