import axios from 'axios';
import config from '../config/config.js';
import logger from './logger.js';

const DATA_URL_REGEX = /^data:([^;]+);base64,(.+)$/;

function resolveProxy() {
  if (!config.proxy) return null;
  try {
    const proxyUrl = new URL(config.proxy);
    if (proxyUrl.protocol !== 'http:' && proxyUrl.protocol !== 'https:') {
      return null;
    }
    return {
      protocol: proxyUrl.protocol.replace(':', ''),
      host: proxyUrl.hostname,
      port: parseInt(proxyUrl.port, 10)
    };
  } catch {
    return null;
  }
}

/**
 * Fetch remote file content and return base64 payload for Claude document blocks.
 *
 * @param {string} url - File URL or data URL
 * @param {string} mimeType - Fallback MIME type
 * @param {object} [options]
 * @param {number} [options.timeoutMs]
 * @param {number} [options.maxBytes]
 * @returns {Promise<{ data: string, mimeType: string }|null>}
 */
export async function fetchFileAsBase64(url, mimeType, options = {}) {
  if (!url || typeof url !== 'string') return null;

  const match = url.match(DATA_URL_REGEX);
  if (match) {
    return { data: match[2], mimeType: match[1] || mimeType || 'application/octet-stream' };
  }

  if (!url.startsWith('http://') && !url.startsWith('https://')) {
    return null;
  }

  try {
    const axiosConfig = {
      method: 'GET',
      url,
      responseType: 'arraybuffer',
      timeout: Number.isFinite(options.timeoutMs) ? options.timeoutMs : config.timeout
    };

    const proxy = resolveProxy();
    if (proxy) {
      axiosConfig.proxy = proxy;
    }

    const response = await axios(axiosConfig);
    const buffer = Buffer.from(response.data || []);

    if (Number.isFinite(options.maxBytes) && buffer.length > options.maxBytes) {
      logger.warn(`File too large to inline (${buffer.length} bytes): ${url}`);
      return null;
    }

    const contentType = response.headers?.['content-type'];
    const normalizedType = contentType ? contentType.split(';')[0].trim() : '';

    return {
      data: buffer.toString('base64'),
      mimeType: normalizedType || mimeType || 'application/octet-stream'
    };
  } catch (error) {
    logger.warn('Failed to fetch file content:', error?.message || error);
    return null;
  }
}

export default {
  fetchFileAsBase64
};
