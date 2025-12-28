/**
 * Token 估算工具模块
 * 提取自 sseUtils.js，提供统一的 Token 数量估算及模型差异化配置
 */

/**
 * 模型 Token 比率配置 (1字符对应的 Token 数)
 *
 * 当前统一使用 0.25 比率以确保向后兼容 (等效于 Math.ceil(length / 4))
 * 未来可根据需要调整不同模型的比率：
 * - GPT 系列: 通常 1 token ≈ 4 字符 (0.25)
 * - Claude 系列: 官方建议 1 token ≈ 3.5 字符 (约 0.28)
 */
export const MODEL_TOKEN_RATIOS = {
  DEFAULT: 0.25,
  GPT: 0.25,
  CLAUDE: 0.25
};

/**
 * 估算文本 token 数量
 * @param {string|object} text - 文本或对象
 * @param {string} [model] - 模型名称 (用于选择比率)
 * @returns {number} - 估算的 token 数量
 */
export function estimateTokensFromText(text, model = null) {
  if (!text) return 0;
  const normalized = typeof text === 'string' ? text : JSON.stringify(text);

  let ratio = MODEL_TOKEN_RATIOS.DEFAULT;
  if (model) {
    const m = model.toLowerCase();
    if (m.includes('claude')) {
      ratio = MODEL_TOKEN_RATIOS.CLAUDE;
    } else if (m.includes('gpt') || m.includes('o1') || m.includes('o3')) {
      ratio = MODEL_TOKEN_RATIOS.GPT;
    }
  }

  // 使用乘法计算: length * 0.25 等效于 length / 4
  return Math.max(1, Math.ceil(normalized.length * ratio));
}

/**
 * 通用 Token 估算函数 (estimateTokensFromText 的别名)
 * @param {string|object} text - 文本或对象
 * @param {string} [model] - 模型名称
 * @returns {number} - 估算的 token 数量
 */
export const estimateTokens = estimateTokensFromText;
