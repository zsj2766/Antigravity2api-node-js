/**
 * Bridge ID 生成 + Token 估算 + 字符串工具
 */

import { randomUUID } from 'crypto';

// --- ID Generation ---

export function generateRequestId() {
  return `agent-${randomUUID()}`;
}

export function generateToolCallId() {
  return `call_${randomUUID().replace(/-/g, '')}`;
}

export function generateToolUseId() {
  return `toolu_${randomUUID().replace(/-/g, '')}`;
}

// --- Token Utils ---

const DEFAULT_TOKEN_RATIO = 0.25;

/**
 * 估算文本 token 数量
 * @param {string|object} text - 文本或对象
 * @returns {number} - 估算的 token 数量
 */
export function estimateTokens(text) {
  if (!text) return 0;
  const normalized = typeof text === 'string' ? text : JSON.stringify(text);
  return Math.max(1, Math.ceil(normalized.length * DEFAULT_TOKEN_RATIO));
}

// --- String Chunking Utils ---

/**
 * 安全地将字符串分割成固定大小的块，确保不切断多字节字符
 *
 * 设计原则（跨语言通用）：
 * 1. JavaScript: String.slice() 按 Unicode 码点切割，本身安全
 * 2. Python: str[:n] 按字符切割安全，但 bytes[:n] 会切断 UTF-8
 * 3. Go/Rust: 需要按 rune/char 边界切割，而非字节
 *
 * 此函数在 JS 中按字符切割，对于需要移植到其他语言的场景，
 * 提供了明确的边界检测逻辑作为参考。
 *
 * @param {string} str - 要分割的字符串
 * @param {number} chunkSize - 每块的最大字符数（非字节数）
 * @returns {string[]} - 分割后的字符串数组
 *
 * @example
 * // JS 中安全切割包含 emoji 的字符串
 * safeChunkString('Hello 📚 World', 7)
 * // => ['Hello 📚', ' World']
 *
 * @example
 * // Python 移植参考（字符级切割）：
 * // def safe_chunk_string(s: str, chunk_size: int) -> list[str]:
 * //     return [s[i:i+chunk_size] for i in range(0, len(s), chunk_size)]
 *
 * @example
 * // Python 移植参考（字节级切割，需要额外处理）：
 * // def safe_chunk_bytes(s: str, byte_size: int) -> list[str]:
 * //     encoded = s.encode('utf-8')
 * //     chunks = []
 * //     i = 0
 * //     while i < len(encoded):
 * //         end = min(i + byte_size, len(encoded))
 * //         # 回退到有效的 UTF-8 边界
 * //         while end > i and (encoded[end-1] & 0xC0) == 0x80:
 * //             end -= 1
 * //         if end == i:  # 单个字符超过 chunk_size
 * //             end = i + byte_size
 * //         chunks.append(encoded[i:end].decode('utf-8', errors='ignore'))
 * //         i = end
 * //     return chunks
 */
export function safeChunkString(str, chunkSize = 128) {
  if (!str || typeof str !== 'string') return [];
  if (chunkSize <= 0) return [str];

  const chunks = [];

  // JavaScript 的 String.prototype.slice() 按 Unicode 码点切割
  // 对于 BMP 外的字符（如 emoji），JS 会正确处理 surrogate pairs
  // 但为了代码的可移植性和明确性，我们使用 Array.from 来按完���字符迭代
  const chars = Array.from(str);  // 按 Unicode 字符分解，正确处理 emoji

  for (let i = 0; i < chars.length; i += chunkSize) {
    chunks.push(chars.slice(i, i + chunkSize).join(''));
  }

  return chunks;
}

/**
 * 安全地将字符串分割成固定字节大小的块（UTF-8 编码）
 * 确保不在 UTF-8 多字节序列中间切断
 *
 * 适用场景：当下游协议有字节大小限制时使用
 *
 * UTF-8 编码规则：
 * - 1字节: 0xxxxxxx (ASCII)
 * - 2字节: 110xxxxx 10xxxxxx
 * - 3字节: 1110xxxx 10xxxxxx 10xxxxxx
 * - 4字节: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
 *
 * 判断方法：如果字节以 10xxxxxx (0x80-0xBF) 开头，说明是延续字节
 *
 * @param {string} str - 要分割的字符串
 * @param {number} byteSize - 每块的最大字节数
 * @returns {string[]} - 分割后的字符串数组
 */
export function safeChunkByBytes(str, byteSize = 128) {
  if (!str || typeof str !== 'string') return [];
  if (byteSize <= 0) return [str];

  const encoder = new TextEncoder();
  const encoded = encoder.encode(str);
  const decoder = new TextDecoder();
  const chunks = [];

  let i = 0;
  while (i < encoded.length) {
    let end = Math.min(i + byteSize, encoded.length);

    // 如果切割点在 UTF-8 多字节序列中间，向前回退到字符边界
    // UTF-8 延续字节的格式是 10xxxxxx (0x80 - 0xBF)
    while (end > i && (encoded[end] & 0xC0) === 0x80) {
      end--;
    }

    // 边界情况：单个字符的字节数超过 byteSize（理论上 UTF-8 最多 4 字节）
    if (end === i) {
      // 找到下一个字符边界
      end = i + 1;
      while (end < encoded.length && (encoded[end] & 0xC0) === 0x80) {
        end++;
      }
    }

    chunks.push(decoder.decode(encoded.slice(i, end)));
    i = end;
  }

  return chunks;
}
