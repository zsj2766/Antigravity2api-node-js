/**
 * 图片模型处理工具函数
 *
 * 提供图片模型相关的参数解析和请求配置。
 *
 * @module utils/imageModelUtils
 */

/**
 * 检查用户是否指定了图片尺寸参数
 *
 * @param {Object} params - 请求参数
 * @returns {boolean}
 */
export function userHasImageSizeParam(params) {
  return !!(
    params.image_size ||
    params.imageSize ||
    params?.generation_config?.image_size ||
    params?.generation_config?.imageSize ||
    params?.generation_config?.image_config?.image_size ||
    params?.generation_config?.image_config?.imageSize ||
    params?.generationConfig?.image_size ||
    params?.generationConfig?.imageSize ||
    params?.generationConfig?.image_config?.image_size ||
    params?.generationConfig?.image_config?.imageSize
  );
}

/**
 * 配置图片模型请求体
 *
 * @param {Object} requestBody - 请求体
 * @param {Object} params - 用户参数
 */
export function configureImageModelRequest(requestBody, params) {
  const userGenerationConfig = params.generation_config || params.generationConfig || {};
  const userImageConfig =
    params.image_config ||
    params.imageConfig ||
    userGenerationConfig.image_config ||
    userGenerationConfig.imageConfig ||
    {};

  const aspectRatio =
    params.aspect_ratio ||
    params.aspectRatio ||
    userImageConfig.aspect_ratio ||
    userImageConfig.aspectRatio;
  const imageSize =
    params.image_size ||
    params.imageSize ||
    userImageConfig.image_size ||
    userImageConfig.imageSize;
  const responseModalities =
    params.response_modalities ||
    params.responseModalities ||
    userGenerationConfig.response_modalities ||
    userGenerationConfig.responseModalities;

  const mergedImageConfig = {};
  if (aspectRatio) mergedImageConfig.aspectRatio = aspectRatio;
  if (imageSize) mergedImageConfig.imageSize = imageSize;

  const mergedGenerationConfig = {
    ...requestBody.request.generationConfig,
    ...userGenerationConfig,
    responseModalities: responseModalities || ["TEXT", "IMAGE"],
    thinkingConfig: {
      includeThoughts: true,
      thinkingBudget: 1024
    },
    candidateCount: 1
  };

  if (Object.keys(mergedImageConfig).length > 0) {
    mergedGenerationConfig.imageConfig = mergedImageConfig;
  }

  requestBody.request.generationConfig = mergedGenerationConfig;
  requestBody.requestType = 'image_gen';
  requestBody.request.systemInstruction.parts[0].text +=
    '（当前作为图像生成模型使用，请根据描述生成图片）';
  delete requestBody.request.tools;
  delete requestBody.request.toolConfig;
}

/**
 * 处理图片模型流式响应
 *
 * @param {Object} options - 配置选项
 * @param {Object} options.requestBody - 请求体
 * @param {Object} options.token - 凭证
 * @param {Object} options.res - Express 响应对象
 * @param {string} options.id - 请求 ID
 * @param {number} options.created - 创建时间戳
 * @param {string} options.model - 模型名称
 * @param {Function} options.generateImageModelResponse - 图片生成函数
 * @param {Function} options.setStreamHeaders - 设置流式响应头
 * @param {Function} options.writeStreamData - 写入流数据
 * @param {Function} options.createStreamChunk - 创建流数据块
 * @param {Function} options.endStream - 结束流
 * @returns {Promise<{ usage: Object, streamEvents: Array, imageUrls: Array }>}
 */
export async function handleImageModelStream({
  requestBody,
  token,
  res,
  id,
  created,
  model,
  generateImageModelResponse,
  setStreamHeaders,
  writeStreamData,
  createStreamChunk,
  endStream
}) {
  const imageUrls = [];
  const streamEvents = [];
  let hasStarted = false;

  const { usage } = await generateImageModelResponse(requestBody, token, data => {
    if (!res.headersSent) setStreamHeaders(res);

    if (!hasStarted) {
      hasStarted = true;
      writeStreamData(res, createStreamChunk(id, created, model, { role: 'assistant', content: '' }));
    }

    streamEvents.push(data);

    if (data.type === 'thinking') {
      writeStreamData(res, createStreamChunk(id, created, model, { reasoning_content: data.content }));
    } else if (data.type === 'image') {
      imageUrls.push(data.url);
    } else if (data.type === 'text') {
      writeStreamData(res, createStreamChunk(id, created, model, { content: data.content }));
    }
  });

  if (imageUrls.length > 0) {
    const markdown = imageUrls.map(url => `![image](${url})`).join('\n\n');
    writeStreamData(res, createStreamChunk(id, created, model, { content: markdown }));
  }

  if (!res.headersSent) setStreamHeaders(res);
  endStream(res, id, created, model, 'stop', usage);

  return { usage, streamEvents, imageUrls };
}

export default {
  userHasImageSizeParam,
  configureImageModelRequest,
  handleImageModelStream
};
