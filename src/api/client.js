import axios from 'axios';
import http from 'http';
import https from 'https';
import tokenManager from '../auth/token_manager.js';
import config from '../config/config.js';
import { log } from '../utils/logger.js';
import { generateRequestId, generateToolCallId } from '../utils/idGenerator.js';
import AntigravityRequester from '../AntigravityRequester.js';
import { saveBase64Image } from '../utils/imageStorage.js';
import { registerTextThoughtSignature, registerThoughtSignature } from '../utils/utils.js';

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

        const status = statusFromStatusText(errorObj.code || errorObj.status);
        const retryDelayMs = parseRetryDelayMs(errorObj, errorObj.message || body);

        return {
            status,
            message: JSON.stringify(errorObj, null, 2),
            retryDelayMs,
            disableToken: status === 401
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
    }

    return {
        status: status ?? 'Unknown',
        message,
        retryDelayMs,
        disableToken,
        rawResponse
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

    const err = new Error(`API请求失败 (${details.status}): ${details.message}`);
    err.status = details.status;
    err.retryAfter = details.retryDelayMs;  // 暴露重试延迟（毫秒）
    err.code = details.status === 429 ? 'RATE_LIMITED' : 'API_ERROR';
    err.rawResponse = details.rawResponse;
    throw err;
}

// 转换 functionCall 为 OpenAI 格式
function convertToToolCall(functionCall) {
    return {
        id: functionCall.id || generateToolCallId(),
        type: 'function',
        function: {
            name: functionCall.name,
            arguments: JSON.stringify(functionCall.args)
        }
    };
}


// 辅助函数：在保留原有结构的同时记录 thoughtSignature
function convertToToolCallWithSignature(functionCall, thoughtSignature) {
    const toolCall = convertToToolCall(functionCall);
    if (thoughtSignature && toolCall && toolCall.id) {
        registerThoughtSignature(toolCall.id, thoughtSignature);
    }
    return toolCall;
}

// 解析并发送流式响应片段（会修改 state 并触发 callback）
function toOpenAiUsage(usageMetadata) {
    if (!usageMetadata) return null;

    const prompt = usageMetadata.promptTokenCount ?? usageMetadata.inputTokenCount ?? null;
    const completion = usageMetadata.candidatesTokenCount ?? usageMetadata.outputTokenCount ?? null;
    const total =
        usageMetadata.totalTokenCount ??
        (Number.isFinite(prompt) && Number.isFinite(completion) ? prompt + completion : null);
    const inferredCompletion =
        completion ?? (Number.isFinite(total) && Number.isFinite(prompt) ? Math.max(total - prompt, 0) : total);

    return {
        prompt_tokens: prompt,
        completion_tokens: inferredCompletion,
        total_tokens:
            total ?? (Number.isFinite(prompt) && Number.isFinite(inferredCompletion) ? prompt + inferredCompletion : null)
    };
}

function flushTextAccumulator(state) {
    if (!state?.textAccumulator) return;
    const { text, signature } = state.textAccumulator;
    if (text && signature) {
        registerTextThoughtSignature(text, signature);
    }
    state.textAccumulator = { text: '', signature: null };
}

function parseAndEmitStreamChunk(line, state, callback) {
    if (!line.startsWith('data: ')) return;

    try {
        const data = JSON.parse(line.slice(6));
        const parts = data.response?.candidates?.[0]?.content?.parts;

        if (data.response?.usageMetadata) {
            state.usage = toOpenAiUsage(data.response.usageMetadata);
        }

        if (parts) {
            for (const part of parts) {
                if (part.thought === true) {
                    // 思维链内容 - 直接发送
                    if (part.text) {
                        callback({ type: 'thinking', content: part.text });
                    }
                    // 思维阶段的中间图片，跳过（只发送最终图片）
                } else if (part.text !== undefined) {
                    if (part.thoughtSignature) {
                        registerTextThoughtSignature(part.text, part.thoughtSignature);
                        state.textAccumulator.signature = part.thoughtSignature;
                    }
                    state.textAccumulator.text += part.text || '';
                    callback({ type: 'text', content: part.text });
                } else if (part.functionCall) {
                    // 工具调用
                    state.toolCalls.push(convertToToolCallWithSignature(part.functionCall, part.thoughtSignature));
                } else if (part.inlineData) {
                    // 图片数据
                    const imageUrl = saveBase64Image(part.inlineData.data, part.inlineData.mimeType);
                    callback({
                        type: 'image',
                        url: imageUrl,
                        mimeType: part.inlineData.mimeType,
                        data: part.inlineData.data,
                        thought: part.thought === true
                    });
                }
            }
        }

        // 响应结束时发送工具调用
        if (data.response?.candidates?.[0]?.finishReason) {
            flushTextAccumulator(state);
            if (state.toolCalls.length > 0) {
                callback({ type: 'tool_calls', tool_calls: state.toolCalls });
                state.toolCalls = [];
            }
        }
    } catch (e) {
        // 忽略 JSON 解析错误
    }
}

// ==================== 导出函数 ====================

export async function generateAssistantResponse(requestBody, token, callback) {

    const state = { toolCalls: [], usage: null, textAccumulator: { text: '', signature: null } };
    let buffer = ''; // 缓冲区：处理跨 chunk 的不完整行
    let streamChunks = []; // 收集流式响应（用于 debug=high 日志）

    const processChunk = (chunk) => {
        buffer += chunk;
        streamChunks.push(chunk); // 收集响应片段
        const lines = buffer.split('\n');
        buffer = lines.pop(); // 保留最后一行（可能不完整）
        lines.forEach(line => parseAndEmitStreamChunk(line, state, callback));
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

    return { usage: state.usage };
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
    const parts = data.response?.candidates?.[0]?.content?.parts || [];
    const usage = toOpenAiUsage(data.response?.usageMetadata);
    let content = '';
    let thinkingContent = '';
    const toolCalls = [];
    const imageUrls = [];

    for (const part of parts) {
        if (part.thought === true) {
            thinkingContent += part.text || '';
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
            // 保存图片到本地并获取 URL
            const imageUrl = saveBase64Image(part.inlineData.data, part.inlineData.mimeType);
            imageUrls.push(imageUrl);
        }
    }

    // 拼接思维链标签（用于非图像模型的普通响应）
    if (thinkingContent && imageUrls.length === 0) {
        content = `<think>\n${thinkingContent}\n</think>\n${content}`;
    }
    if (aggregatedText && aggregatedTextSignature) {
        registerTextThoughtSignature(aggregatedText, aggregatedTextSignature);
    }

    // 生图模型：转换为 markdown 格式，并返回独立的 thinking 字段
    if (imageUrls.length > 0) {
        let markdown = content ? content + '\n\n' : '';
        markdown += imageUrls.map(url => `![image](${url})`).join('\n\n');
        return { content: markdown, toolCalls, thinking: thinkingContent || null };
    }

    return { content, toolCalls, usage };
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
