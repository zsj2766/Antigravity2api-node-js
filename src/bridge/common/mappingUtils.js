/**
 * Bridge Thinking 配置 + Stop Reason 映射工具
 *
 * Thinking 映射:
 * | Effort Level | Budget Tokens |
 * |--------------|---------------|
 * | low          | 5000          |
 * | medium       | 10000         |
 * | high         | 20000         |
 *
 * Stop Reason 映射:
 * | 原因     | Claude         | OpenAI         | Gemini                    |
 * |----------|----------------|----------------|---------------------------|
 * | 正常结束 | end_turn       | stop           | STOP                      |
 * | 长度限制 | max_tokens     | length         | MAX_TOKENS                |
 * | 工具调用 | tool_use       | tool_calls     | STOP (需检测 content)     |
 * | 内容过滤 | content_filter | content_filter | SAFETY/RECITATION/...     |
 * | 停止序列 | stop_sequence  | stop           | STOP                      |
 */

// ============================================================================
// Thinking Config
// ============================================================================

/**
 * Effort Level → Budget Tokens 映射
 *
 * 用途：将 OpenAI reasoning_effort 或 Claude thinking_level 转换为 Gemini thinkingBudget
 */
const EFFORT_TO_BUDGET = {
  low: 5000,
  medium: 10000,
  high: 20000
};

const DEFAULT_EFFORT = 'medium';
const DEFAULT_BUDGET = 10000;

/**
 * 模型 Thinking Budget 限制配置表
 *
 * 背景：不同模型对 thinkingBudget 有不同的 min/max 限制。
 * CLIProxyAPI 使用 ModelRegistry 动态获取，这里使用硬编码常量表作为兜底。
 *
 * 参考：CLIProxyAPI util.NormalizeThinkingBudget
 *
 * 配置说明：
 * - minBudget: 最小 budget（低于此值会移除 thinking 配置）
 * - maxBudget: 最大 budget（超过此值会被截断）
 * - useLevel: 是否使用 thinkingLevel 而非 thinkingBudget（Gemini 3 系列）
 */
export const MODEL_THINKING_LIMITS = {
  // Gemini 3 系列：使用 thinkingLevel (LOW/MEDIUM/HIGH)
  'gemini-3': {
    useLevel: true,
    defaultLevel: 'HIGH'
  },

  // Gemini 2.0 Flash Thinking 系列
  'gemini-2.0-flash-thinking': {
    minBudget: 1024,
    maxBudget: 24576,
    defaultBudget: 8192
  },

  // Gemini 2.5 Pro/Flash (通用)
  'gemini-2.5': {
    minBudget: 1024,
    maxBudget: 24576,
    defaultBudget: 10000
  },

  // Claude 3.7 Sonnet Thinking
  'claude-3-7-sonnet': {
    minBudget: 1024,
    maxBudget: 128000,  // 实际受 maxOutputTokens 限制
    defaultBudget: 10000
  },

  // Claude 4 系列
  'claude-sonnet-4': {
    minBudget: 1024,
    maxBudget: 128000,
    defaultBudget: 10000
  },
  'claude-opus-4': {
    minBudget: 1024,
    maxBudget: 128000,
    defaultBudget: 10000
  },

  // 默认配置（未知模型）
  '_default': {
    minBudget: 1024,
    maxBudget: 24576,
    defaultBudget: 10000
  }
};

/**
 * 获取模型的 Thinking 限制配置
 *
 * 策略：使用前缀匹配，找到最长匹配的配置
 *
 * @param {string} modelName - 模型名称
 * @returns {object} 模型的 Thinking 限制配置
 */
export function getThinkingLimits(modelName) {
  if (!modelName || typeof modelName !== 'string') {
    return MODEL_THINKING_LIMITS['_default'];
  }

  const lowerModel = modelName.toLowerCase();

  // 按前缀长度降序排列，优先匹配更具体的配置
  const prefixes = Object.keys(MODEL_THINKING_LIMITS)
    .filter(k => k !== '_default')
    .sort((a, b) => b.length - a.length);

  for (const prefix of prefixes) {
    if (lowerModel.includes(prefix.toLowerCase())) {
      return MODEL_THINKING_LIMITS[prefix];
    }
  }

  return MODEL_THINKING_LIMITS['_default'];
}

/**
 * 规范化 Thinking Budget
 *
 * 功能：
 * 1. 将 budget 限制在模型支持的 min/max 范围内
 * 2. 对于 Claude 模型，确保 budget < maxOutputTokens
 * 3. 如果 budget 低于最小值，返回 null 表示应移除 thinking 配置
 *
 * 参考：CLIProxyAPI normalizeAntigravityThinking
 *
 * @param {string} modelName - 模型名称
 * @param {number} budget - 原始 budget
 * @param {number} maxOutputTokens - 可选的 maxOutputTokens 限制
 * @returns {number|null} 规范化后的 budget，或 null 表示应禁用 thinking
 */
export function normalizeThinkingBudget(modelName, budget, maxOutputTokens = 0) {
  const limits = getThinkingLimits(modelName);

  // 如果模型使用 thinkingLevel，不进行 budget 规范化
  if (limits.useLevel) {
    return budget;
  }

  let normalized = budget;

  // 应用 min/max 限制
  if (limits.minBudget && normalized < limits.minBudget) {
    // 低于最小值，返回 null 表示应禁用 thinking
    return null;
  }
  if (limits.maxBudget && normalized > limits.maxBudget) {
    normalized = limits.maxBudget;
  }

  // Claude 特有约束：budget 必须 < maxOutputTokens
  // 参考：CLIProxyAPI antigravity_executor.go
  const isClaude = modelName && modelName.toLowerCase().includes('claude');
  if (isClaude && maxOutputTokens > 0) {
    if (normalized >= maxOutputTokens) {
      normalized = maxOutputTokens - 1;
    }
    // 如果调整后仍低于最小值，禁用 thinking
    if (limits.minBudget && normalized < limits.minBudget) {
      return null;
    }
  }

  return normalized;
}

/**
 * 判断模型是否应使用 thinkingLevel 而非 thinkingBudget
 *
 * 原因：Gemini 3 系列使用 thinkingLevel (LOW/MEDIUM/HIGH)，
 * 其他模型使用 thinkingBudget (数值)。
 *
 * 参考：CLIProxyAPI convertThinkingLevelToBudget
 *
 * @param {string} modelName - 模型名称
 * @returns {boolean} 是否使用 thinkingLevel
 */
export function shouldUseThinkingLevel(modelName) {
  const limits = getThinkingLimits(modelName);
  return limits.useLevel === true;
}

/**
 * 将 reasoning_effort (low/medium/high) 转换为 budget_tokens
 */
export function resolveThinkingBudget(effort) {
  if (typeof effort === 'number') return effort;
  return EFFORT_TO_BUDGET[effort] || DEFAULT_BUDGET;
}

/**
 * 将 budget_tokens 转换为 reasoning_effort (low/medium/high)
 */
export function resolveReasoningEffort(tokens) {
  if (!tokens || typeof tokens !== 'number') return DEFAULT_EFFORT;
  if (tokens < 7500) return 'low';
  if (tokens < 15000) return 'medium';
  return 'high';
}

// --- Stop Reason Mapping ---

const STOP_REASON_MAP = {
  GEMINI: {
    STOP: { claude: 'end_turn', openai: 'stop' },
    MAX_TOKENS: { claude: 'max_tokens', openai: 'length' },
    SAFETY: { claude: 'content_filter', openai: 'content_filter' },
    RECITATION: { claude: 'content_filter', openai: 'content_filter' },
    LANGUAGE: { claude: 'content_filter', openai: 'content_filter' },
    BLOCKLIST: { claude: 'content_filter', openai: 'content_filter' },
    PROHIBITED_CONTENT: { claude: 'content_filter', openai: 'content_filter' },
    SPII: { claude: 'content_filter', openai: 'content_filter' },
    MALFORMED_FUNCTION_CALL: { claude: 'end_turn', openai: 'stop' },
    OTHER: { claude: 'end_turn', openai: 'stop' }
  },
  CLAUDE: {
    end_turn: 'stop',
    stop_sequence: 'stop',
    max_tokens: 'length',
    tool_use: 'tool_calls',
    content_filter: 'content_filter'
  },
  OPENAI: {
    stop: 'end_turn',
    length: 'max_tokens',
    tool_calls: 'tool_use',
    content_filter: 'content_filter',
    function_call: 'tool_use'
  }
};

/**
 * 将 Gemini finishReason 映射为 Claude/OpenAI 格式
 */
export function mapGeminiStopReason(finishReason, hasToolCalls) {
  const reason = finishReason || 'STOP';
  if (reason === 'STOP' && hasToolCalls) {
    return { claude: 'tool_use', openai: 'tool_calls' };
  }
  return STOP_REASON_MAP.GEMINI[reason] || STOP_REASON_MAP.GEMINI.OTHER;
}

/**
 * 将 Claude stop_reason 映射为 OpenAI finish_reason
 */
export function mapClaudeStopToOpenAI(stopReason) {
  return STOP_REASON_MAP.CLAUDE[stopReason] || 'stop';
}

/**
 * 将 OpenAI finish_reason 映射为 Claude stop_reason
 */
export function mapOpenAIFinishToClaude(finishReason) {
  return STOP_REASON_MAP.OPENAI[finishReason] || 'end_turn';
}
