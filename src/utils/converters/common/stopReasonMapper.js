/**
 * Stop Reason / Finish Reason 统一映射模块
 *
 * 职责：
 * 1. 集中管理 Gemini/Claude/OpenAI 三方 stop_reason 映射
 * 2. 提供双向映射函数
 * 3. 处理未知值的降级策略
 *
 * 映射表：
 * | 原因     | Claude         | OpenAI         | Gemini                    |
 * |----------|----------------|----------------|---------------------------|
 * | 正常结束 | end_turn       | stop           | STOP                      |
 * | 长度限制 | max_tokens     | length         | MAX_TOKENS                |
 * | 工具调用 | tool_use       | tool_calls     | STOP (需检测 content)     |
 * | 内容过滤 | content_filter | content_filter | SAFETY/RECITATION/...     |
 * | 停止序列 | stop_sequence  | stop           | STOP                      |
 */

/**
 * 映射表常量
 * - GEMINI: Gemini finishReason → {claude, openai} 对象
 * - CLAUDE: Claude stop_reason → OpenAI finish_reason
 * - OPENAI: OpenAI finish_reason → Claude stop_reason
 */
export const STOP_REASON_MAP = {
  // Gemini -> Standard (Claude/OpenAI)
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
  // Claude -> OpenAI
  CLAUDE: {
    end_turn: 'stop',
    stop_sequence: 'stop',
    max_tokens: 'length',
    tool_use: 'tool_calls',
    content_filter: 'content_filter'
  },
  // OpenAI -> Claude
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
 *
 * 特殊处理：Gemini 返回 STOP 时，如果包含工具调用，则映射为 tool_use/tool_calls
 *
 * @param {string} finishReason - Gemini candidate.finishReason
 * @param {boolean} hasToolCalls - 响应中是否包含工具调用
 * @returns {{claude: string, openai: string}} - 映射后的停止原因
 */
export function mapGeminiStopReason(finishReason, hasToolCalls) {
  // 默认为 STOP（如果 undefined）
  const reason = finishReason || 'STOP';

  // 特殊处理：STOP + 工具调用 = tool_use/tool_calls
  if (reason === 'STOP' && hasToolCalls) {
    return { claude: 'tool_use', openai: 'tool_calls' };
  }

  // 查表映射，未知值降级为 OTHER
  const mapping = STOP_REASON_MAP.GEMINI[reason] || STOP_REASON_MAP.GEMINI.OTHER;
  return mapping;
}

/**
 * 将 Claude stop_reason 映射为 OpenAI finish_reason
 *
 * @param {string} stopReason - Claude stop_reason
 * @returns {string} - OpenAI finish_reason，未知值降级为 'stop'
 */
export function mapClaudeToOpenAI(stopReason) {
  return STOP_REASON_MAP.CLAUDE[stopReason] || 'stop';
}

/**
 * 将 OpenAI finish_reason 映射为 Claude stop_reason
 *
 * @param {string} finishReason - OpenAI finish_reason
 * @returns {string} - Claude stop_reason，未知值降级为 'end_turn'
 */
export function mapOpenAIToClaude(finishReason) {
  return STOP_REASON_MAP.OPENAI[finishReason] || 'end_turn';
}
