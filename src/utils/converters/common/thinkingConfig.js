/**
 * Thinking 配置转换工具
 * 负责 OpenAI reasoning_effort 与 Budget Tokens 之间的标准化映射
 *
 * 映射关系:
 * | Effort Level | Budget Tokens |
 * |--------------|---------------|
 * | low          | 5000          |
 * | medium       | 10000         |
 * | high         | 20000         |
 */

// 映射表：OpenAI Effort -> Token Budget
export const EFFORT_TO_BUDGET = {
  low: 5000,
  medium: 10000,
  high: 20000
};

export const DEFAULT_EFFORT = 'medium';
export const DEFAULT_BUDGET = 10000;

/**
 * 将 reasoning_effort (low/medium/high) 转换为 budget_tokens
 * @param {string|number} effort - effort 字符串或已有 token 数
 * @returns {number}
 */
export function resolveThinkingBudget(effort) {
  if (typeof effort === 'number') return effort;
  return EFFORT_TO_BUDGET[effort] || DEFAULT_BUDGET;
}

/**
 * 将 budget_tokens 转换为 reasoning_effort (low/medium/high)
 * 阈值逻辑：
 * < 7500 -> low
 * >= 7500 && < 15000 -> medium
 * >= 15000 -> high
 * @param {number} tokens - token 数量
 * @returns {string}
 */
export function resolveReasoningEffort(tokens) {
  if (!tokens || typeof tokens !== 'number') return DEFAULT_EFFORT;

  if (tokens < 7500) {
    return 'low';
  }
  if (tokens < 15000) {
    return 'medium';
  }
  return 'high';
}
