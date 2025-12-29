/**
 * MCP 开关标志模块
 *
 * 用于控制 MCP 工具切换功能：
 * - 当检测到 Claude 模型需要使用 mcp__ 工具时，切换到指定的备用模型
 * - 通过环境变量 AG2API_SWITCH_TO_MCP_MODEL 配置
 */

/**
 * MCP 切换信号字符串
 * 当模型输出此信号时，表示需要切换到 MCP 模型
 */
export const MCP_SWITCH_SIGNAL = 'AG2API_SWITCH_TO_MCP_MODEL';

/**
 * 获取 MCP 切换目标模型
 * @returns {string|null} - 目标模型名，如果未配置则返回 null
 */
export function getMcpSwitchModel() {
  const raw = process.env.AG2API_SWITCH_TO_MCP_MODEL;
  if (raw === undefined || raw === null) return null;
  const trimmed = String(raw).trim();
  return trimmed ? trimmed : null;
}

/**
 * 检查 MCP 切换功能是否启用
 * @returns {boolean}
 */
export function isMcpSwitchEnabled() {
  return !!getMcpSwitchModel();
}
