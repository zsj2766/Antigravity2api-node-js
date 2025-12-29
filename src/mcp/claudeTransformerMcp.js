/**
 * Claude Transformer MCP 模块
 *
 * 处理 MCP 工具相关的逻辑：
 * - 检测请求中是否包含 mcp__ 工具
 * - 在 system prompt 中注入 MCP 使用提示
 * - 从 system text 中移除 mcp__ 工具名称（避免 Claude 模型尝试直接调用）
 */

import { MCP_SWITCH_SIGNAL, isMcpSwitchEnabled } from './mcpSwitchFlag.js';

/**
 * 检查请求中是否包含 MCP 工具
 * @param {object} claudeReq - Claude 请求对象
 * @returns {boolean}
 */
export function hasMcpTools(claudeReq) {
  return (
    Array.isArray(claudeReq?.tools) &&
    claudeReq.tools.some((tool) => typeof tool?.name === 'string' && tool.name.startsWith('mcp__'))
  );
}

/**
 * 构建 MCP 工具名称提示（用于注入 system prompt）
 * @param {object} claudeReq - Claude 请求对象
 * @returns {string} - MCP 工具名称模式（如 "mcp__server1__*、mcp__server2__*"）
 */
function buildMcpToolNamesForHint(claudeReq) {
  const tools = Array.isArray(claudeReq?.tools) ? claudeReq.tools : [];
  const servers = new Set();
  for (const tool of tools) {
    const name = tool?.name;
    if (typeof name !== 'string' || !name.startsWith('mcp__')) continue;
    const parts = name.split('__');
    if (parts.length >= 3 && parts[0] === 'mcp' && parts[1]) servers.add(parts[1]);
  }
  const patterns = Array.from(servers)
    .sort()
    .map((server) => `mcp__${server}__*`);
  return patterns.length > 0 ? patterns.join('、') : 'mcp__*';
}

/**
 * 构建 MCP 切换模型提示文本
 * @param {object} claudeReq - Claude 请求对象
 * @returns {string} - 提示文本
 */
function buildMcpSwitchModelHint(claudeReq) {
  const mcpToolNamesForHint = buildMcpToolNamesForHint(claudeReq);
  return (
    `IMPORTANT（MCP 工具使用规则）\n` +
    `当前会话提供了 MCP 工具（名称以 \`mcp__\` 开头，例如：${mcpToolNamesForHint}）。\n` +
    `由于在当前链路下你直接调用任何 \`mcp__*\` 工具会导致严重异常：\n\n` +
    `1) 只要你判断"现在或后续"需要调用任意 \`mcp__*\` 工具，严禁直接调用工具；也严禁输出其它任何内容（包括解释、计划、TodoWrite、markdown）。\n` +
    `   即使你在本会话中曾成功调用过 \`mcp__*\` 工具，也必须严格遵守以上规则。\n` +
    `2) 此时你必须只输出下面这一行特殊字符串（完全一致，单独一行，不要添加任何前后缀/标点/空格/其它字符）：\n` +
    `${MCP_SWITCH_SIGNAL}\n\n` +
    `不需要使用 \`mcp__*\` 工具时，请正常回答用户。`
  );
}

/**
 * 向 system text 注入 MCP 使用提示
 *
 * 条件：
 * 1. MCP 切换功能已启用（环境变量 AG2API_SWITCH_TO_MCP_MODEL 已设置）
 * 2. 是 Claude 模型
 * 3. 请求中包含 mcp__ 工具
 * 4. system text 中包含 "mcp__" 字符串
 *
 * @param {object} options
 * @param {string} options.text - 原始 system text
 * @param {object} options.claudeReq - Claude 请求对象
 * @param {boolean} options.isClaudeModel - 是否为 Claude 模型
 * @param {boolean} options.injected - 是否已注入过提示
 * @returns {{ text: string, injected: boolean }}
 */
export function maybeInjectMcpHintIntoSystemText({ text, claudeReq, isClaudeModel, injected }) {
  if (!isMcpSwitchEnabled()) return { text, injected };
  if (!isClaudeModel) return { text, injected };
  if (!hasMcpTools(claudeReq)) return { text, injected };
  if (typeof text !== 'string' || !text.includes('mcp__')) return { text, injected };

  let nextText = text;

  // 从 system text 中移除明确的 mcp__ 工具名称
  for (const tool of Array.isArray(claudeReq?.tools) ? claudeReq.tools : []) {
    const name = tool?.name;
    if (typeof name === 'string' && name.startsWith('mcp__')) {
      nextText = nextText.replaceAll(name, '');
    }
  }

  // 清理删除后残留的���隔符
  nextText = nextText
    .replace(/,\s*,/g, ', ')
    .replace(/,\s*\n/g, '\n')
    .replace(/,\s*\)/g, ')')
    .replace(/\(\s*,/g, '(')
    .replace(/\s+,/g, ',')
    .replace(/,\s*$/gm, '')
    .replace(/ {2,}/g, ' ');

  // 只注入一次提示
  if (!injected) {
    nextText = `${nextText}\n\n${buildMcpSwitchModelHint(claudeReq)}`;
    return { text: nextText, injected: true };
  }

  return { text: nextText, injected };
}

/**
 * 过滤工具列表，移除 mcp__ 工具（用于 Claude 模型）
 * @param {Array} tools - 工具列表
 * @param {boolean} isClaudeModel - 是否为 Claude 模型
 * @returns {Array} - 过滤后的工具列表
 */
export function filterMcpTools(tools, isClaudeModel) {
  if (!isClaudeModel) return tools;
  if (!Array.isArray(tools)) return tools;

  return tools.filter((tool) => {
    const name = tool?.name;
    return !(typeof name === 'string' && name.startsWith('mcp__'));
  });
}
