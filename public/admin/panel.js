const loginBtn = document.getElementById('loginBtn');
const logoutBtn = document.getElementById('logoutBtn');
const statusEl = document.getElementById('status');
const tomlStatusEl = document.getElementById('tomlStatus');
const listEl = document.getElementById('accountsList');
const refreshBtn = document.getElementById('refreshBtn');
const refreshAllBtn = document.getElementById('refreshAllBtn');
const logsRefreshBtn = document.getElementById('logsRefreshBtn');
const logsClearBtn = document.getElementById('logsClearBtn');
const hourlyUsageEl = document.getElementById('hourlyUsage');
const manageStatusEl = document.getElementById('manageStatus');
const callbackUrlInput = document.getElementById('callbackUrlInput');
const customProjectIdInput = document.getElementById('customProjectIdInput');
const allowRandomProjectIdCheckbox = document.getElementById('allowRandomProjectId');
const submitCallbackBtn = document.getElementById('submitCallbackBtn');
const logsEl = document.getElementById('logs');
const usageStatusEl = document.getElementById('usageStatus');
const settingsGrid = document.getElementById('settingsGrid');
const settingsStatusEl = document.getElementById('settingsStatus');
const settingsRefreshBtn = document.getElementById('settingsRefreshBtn');
const importTomlBtn = document.getElementById('importTomlBtn');
const tomlInput = document.getElementById('tomlInput');
const replaceExistingCheckbox = document.getElementById('replaceExisting');
const filterDisabledCheckbox = document.getElementById('filterDisabled');
const tabButtons = document.querySelectorAll('.tab-btn');
const tabPanels = document.querySelectorAll('.tab-panel');
const deleteDisabledBtn = document.getElementById('deleteDisabledBtn');
const usageRefreshBtn = document.getElementById('usageRefreshBtn');
const loadAllQuotasBtn = document.getElementById('loadAllQuotasBtn');
const paginationInfo = document.getElementById('paginationInfo');
const prevPageBtn = document.getElementById('prevPageBtn');
const nextPageBtn = document.getElementById('nextPageBtn');
const logPaginationInfo = document.getElementById('logPaginationInfo');
const logPrevPageBtn = document.getElementById('logPrevPageBtn');
const logNextPageBtn = document.getElementById('logNextPageBtn');
const themeToggleBtn = document.getElementById('themeToggleBtn');

const HOUR_WINDOW_MINUTES = 60;
const HOURLY_LIMIT = 20;

const PAGE_SIZE = 20;
let accountsData = [];
let tokenRuntimeStats = {};
let tokenCooldownMs = 5 * 60 * 1000; // 默认5分钟，从后端动态更新
let tokenConfig = {
  cooldownMs: 300000,
  maxStickyUsage: 5,
  poolSize: 3,
  hourlyLimit: 20
};
let filteredAccounts = [];
let currentPage = 1;
const LOG_PAGE_SIZE = 20;
let logsData = [];
let filteredLogs = [];
let logCurrentPage = 1;
let logFilters = {
  type: 'all',
  status: 'all',
  time: 'all',
  customStart: null,
  customEnd: null,
  correlationId: null
};
let statusFilter = 'all';
let errorOnly = false;
const logDetailCache = new Map();

let logLevelSelect = null;
let replaceIndex = null;

if (window.AgTheme) {
  window.AgTheme.initTheme();
  window.AgTheme.bindThemeToggle(themeToggleBtn);
}

// Inject styles for error preview
const errorPreviewStyle = document.createElement('style');
errorPreviewStyle.textContent = `
  .log-error-preview {
    margin-top: 6px;
    padding: 6px;
    background: var(--subtle-bg, #f3f4f6);
    border: 1px solid var(--border, #e5e7eb);
    border-radius: 4px;
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 11px;
    white-space: pre-wrap;
    word-break: break-all;
    color: var(--muted, #4b5563);
    max-height: 120px;
    overflow-y: auto;
  }
  .log-item.log-fail .log-error-preview {
    background: rgba(220, 38, 38, 0.05);
    border-color: rgba(220, 38, 38, 0.2);
    color: var(--status-off-text, #dc2626);
  }
  .chip-success {
    background: var(--status-ok-bg);
    color: var(--status-ok-text);
    border-color: var(--status-ok-bg);
  }
  .log-group {
    border-left: 3px solid var(--border, #e5e7eb);
    padding-left: 8px;
    margin-bottom: 8px;
    background-color: var(--subtle-bg, rgba(0, 0, 0, 0.02));
    border-radius: 0 4px 4px 0;
  }
`;
document.head.appendChild(errorPreviewStyle);

function setStatus(text, type = 'info', target = statusEl) {
  if (!target) return;
  if (!text) {
    target.style.display = 'none';
    return;
  }
  target.textContent = text;
  target.className = `badge badge-${type}`;
  target.style.display = 'inline-block';
}

function activateTab(target) {
  tabButtons.forEach(btn => {
    btn.classList.toggle('active', btn.dataset.tabTarget === target);
  });
  tabPanels.forEach(panel => {
    panel.classList.toggle('active', panel.dataset.tab === target);
  });
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, { credentials: 'same-origin', ...options });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.error) {
    throw new Error(data.error || `HTTP ${res.status}`);
  }
  return data;
}

function escapeHtml(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatJson(value) {
  try {
    return escapeHtml(JSON.stringify(value ?? {}, null, 2));
  } catch (e) {
    return escapeHtml(String(value));
  }
}

function getAccountDisplayName(acc) {
  if (!acc) return '未知账号';
  if (acc.email) return acc.email;
  if (acc.user_email) return acc.user_email;
  if (acc.projectId) return acc.projectId;
  if (typeof acc.index === 'number') return `账号 #${acc.index + 1}`;
  return '未知账号';
}

async function loadTokenRuntimeStats() {
  try {
    const data = await fetchJson('/admin/tokens/stats');
    tokenRuntimeStats = data.stats || {};
    if (data.cooldownMs) {
      tokenCooldownMs = data.cooldownMs;
    }
    // 更新配置
    if (data.config) {
      tokenConfig = { ...tokenConfig, ...data.config };
      updateStrategyDisplay();
    }
  } catch (e) {
    console.error('加载运行时统计失败:', e);
  }
}

function updateStrategyDisplay() {
  const rulesEl = document.querySelector('.strategy-rules');
  if (!rulesEl) return;

  const cooldownMinutes = Math.round(tokenConfig.cooldownMs / 60000);

  rulesEl.innerHTML = `
    <span><strong>过滤规则:</strong> 排除冷却中 / 超限 / 已禁用凭证</span>
    <span><strong>选择策略:</strong> 最久未使用 (LRU) Top ${tokenConfig.poolSize} + 空闲时间加权随机</span>
    <span><strong>连续保护:</strong> 成功调用后锁定 ${tokenConfig.maxStickyUsage} 次 (Sticky Session)</span>
    <span><strong>冷却机制:</strong> 429 错误自动冷却 ${cooldownMinutes} 分钟</span>
    <span><strong>流量限制:</strong> 默认 ${tokenConfig.hourlyLimit} 次/小时/凭证</span>
  `;
}

function renderUsageCard(account) {
  const { usage = {} } = account;
  const models = usage.models && usage.models.length > 0 ? usage.models.map(escapeHtml).join(', ') : '暂无数据';
  const lastUsed = usage.lastUsedAt ? new Date(usage.lastUsedAt).toLocaleString() : '未使用';

  // 运行时统计 - 使用 projectId 作为 key
  const stats = tokenRuntimeStats[account.projectId] || {
    lastUsed: 0,
    lastFailure: 0,
    failureCount: 0,
    successCount: 0,
    inCooldown: false
  };

  // 计算成功率
  const totalReqs = stats.successCount + stats.failureCount;
  const successRate = totalReqs > 0 ? Math.round((stats.successCount / totalReqs) * 100) : 100;
  const rateClass = successRate >= 80 ? 'score-high' : successRate >= 50 ? 'score-medium' : 'score-low';

  // 冷却倒计时
  let cooldownHtml = '';
  if (stats.inCooldown) {
    const cooldownEnd = stats.lastFailure + tokenCooldownMs;
    const remainingSeconds = Math.max(0, Math.ceil((cooldownEnd - Date.now()) / 1000));
    cooldownHtml = `<div class="cooldown-badge">❄️ 冷却中 (${remainingSeconds}s)</div>`;
  }

  return `
    <div class="usage">
      <div class="stats-header">
        <div class="score-badge ${rateClass}" data-tooltip="基于本次运行数据计算\n成功数 / (成功数 + 失败数) × 100%">成功率: ${successRate}%</div>
        ${cooldownHtml}
      </div>
      <div class="usage-grid">
        <div class="usage-cell" data-tooltip="服务启动后的统计，重启后清零\n用于计算成功率和负载均衡">
          <span>本次运行</span>
          <strong>✅${stats.successCount} / ❌${stats.failureCount}</strong>
        </div>
        <div class="usage-cell" data-tooltip="从日志文件统计的历史数据\n受日志保留策略影响（默认保留 7 天）">
          <span>历史统计</span>
          <strong>${usage.total || 0} 次 (成功 ${usage.success || 0} / 失败 ${usage.failed || 0})</strong>
        </div>
        <div class="usage-cell">
          <span>最近使用</span>
          <strong>${lastUsed}</strong>
        </div>
        <div class="usage-cell">
          <span>使用过的模型</span>
          <strong>${models}</strong>
        </div>
      </div>
    </div>
  `;
}

function updateFilteredAccounts() {
  filteredAccounts = accountsData.filter(acc => {
    const matchesStatus =
      statusFilter === 'all' || (statusFilter === 'enabled' && acc.enable) || (statusFilter === 'disabled' && !acc.enable);

    // 检查是否有错误：usage.failed > 0 或者有 error 字段或者运行时统计有失败
    const failedCount = acc?.usage?.failed || 0;
    const stats = tokenRuntimeStats[acc.projectId] || {};
    const runtimeFailed = stats.failureCount || 0;
    const hasError = failedCount > 0 || runtimeFailed > 0 || !!acc.error;
    const matchesError = !errorOnly || hasError;

    return matchesStatus && matchesError;
  });

  currentPage = 1;
  renderAccountsList();
}

async function refreshAllAccountsBatch() {
  if (!accountsData.length) {
    setStatus('暂无凭证可刷新。', 'info', manageStatusEl);
    return;
  }

  if (refreshAllBtn) refreshAllBtn.disabled = true;
  setStatus('正在批量刷新凭证...', 'info', manageStatusEl);

  try {
    const { refreshed = 0, failed = 0 } = await fetchJson('/auth/accounts/refresh-all', { method: 'POST' });
    const message = `批量刷新完成：成功 ${refreshed} 个，失败 ${failed} 个。`;
    setStatus(message, failed > 0 ? 'warning' : 'success', manageStatusEl);
    await refreshAccounts();
  } catch (e) {
    setStatus('批量刷新失败: ' + e.message, 'error', manageStatusEl);
  } finally {
    if (refreshAllBtn) refreshAllBtn.disabled = false;
  }
}

// bindAccountActions 已移除，改用事件委托（见文件末尾）

async function loadQuota(accountIndex, showLoading = false) {
  const quotaSection = document.getElementById(`quota-${accountIndex}`);
  if (!quotaSection) return;

  try {
    if (showLoading) {
      quotaSection.innerHTML = '<div class="quota-loading">加载中...</div>';
    }
    const data = await fetchJson(`/admin/tokens/${accountIndex}/quotas`, { cache: 'no-store' });
    renderQuota(quotaSection, data.data);
  } catch (e) {
    quotaSection.innerHTML = `<div class="quota-error">加载失败: ${e.message}</div>`;
  }
}

function renderQuota(container, quotaData) {
  if (!quotaData || !quotaData.models) {
    container.innerHTML = '<div class="quota-error">暂无额度数据</div>';
    return;
  }

  const lastUpdated = quotaData.lastUpdated ?
    new Date(quotaData.lastUpdated).toLocaleString() : '未知时间';

  // 模型分组配置
  const modelGroups = {
    'Claude/GPT': {
      models: ['claude-sonnet-4-5-thinking', 'claude-opus-4-5-thinking', 'claude-sonnet-4-5', 'gpt-oss-120b-medium'],
      icon: '🧠',
      description: 'Claude和GPT模型共享额度'
    },
    'Tab补全': {
      models: ['chat_23310', 'chat_20706'],
      icon: '📝',
      description: 'Tab补全模型'
    },
    '🍌香蕉绘图': {
      models: ['gemini-2.5-flash-image'],
      icon: '🍌',
      description: 'Gemini图像生成模型'
    },
    '香蕉Pro': {
      models: ['gemini-3-pro-image'],
      icon: '🌟',
      description: 'Gemini Pro图像生成模型'
    },
    'Gemini其他': {
      models: ['gemini-3-pro-high', 'rev19-uic3-1p', 'gemini-2.5-flash', 'gemini-3-pro-low', 'gemini-2.5-flash-thinking', 'gemini-2.5-pro', 'gemini-2.5-flash-lite'],
      icon: '💎',
      description: '其他Gemini模型共享额度'
    }
  };

  // 对模型进行分组
  const groupedModels = {};
  const otherModels = [];

  // 初始化分组
  Object.keys(modelGroups).forEach(groupName => {
    groupedModels[groupName] = {
      ...modelGroups[groupName],
      modelIds: [],
      remaining: [],
      resetTime: null
    };
  });

  // 将模型分配到对应分组
  for (const [modelName, modelInfo] of Object.entries(quotaData.models)) {
    let assigned = false;

    for (const [groupName, groupConfig] of Object.entries(modelGroups)) {
      if (groupConfig.models.includes(modelName)) {
        groupedModels[groupName].modelIds.push(modelName);
        groupedModels[groupName].remaining.push(modelInfo.remaining);
        if (!groupedModels[groupName].resetTime) {
          groupedModels[groupName].resetTime = modelInfo.resetTime;
        }
        assigned = true;
        break;
      }
    }

    if (!assigned) {
      otherModels.push({
        name: modelName,
        remaining: modelInfo.remaining,
        resetTime: modelInfo.resetTime
      });
    }
  }

  // 获取折叠状态，默认为展开
  const isCollapsed = localStorage.getItem('quota-models-collapsed') === 'true';

  let html = `
    <div class="quota-header">
      <span class="quota-title">模型额度信息（分组显示）</span>
      <div class="quota-header-actions">
        <span class="quota-updated">更新时间: ${lastUpdated}</span>
        <button class="quota-toggle-btn" data-collapsed="${isCollapsed}" type="button">
          <span class="quota-toggle-icon">${isCollapsed ? '▶' : '▼'}</span>
          <span class="quota-toggle-text">${isCollapsed ? '展开模型' : '收起模型'}</span>
        </button>
      </div>
    </div>
    <div class="quota-groups" data-collapsed="${isCollapsed}">
  `;

  // 渲染分组模型
  for (const [groupName, groupData] of Object.entries(groupedModels)) {
    if (groupData.modelIds.length === 0) continue;

    // 计算平均剩余额度
    const avgRemaining = groupData.remaining.length > 0
      ? groupData.remaining.reduce((a, b) => a + b, 0) / groupData.remaining.length
      : 0;
    const remainingPercentage = Math.round(avgRemaining * 100);
    const resetTime = groupData.resetTime || '未知时间';
    const colorClass = remainingPercentage > 50 ? 'quota-high' :
                      remainingPercentage > 20 ? 'quota-medium' : 'quota-low';

    html += `
      <div class="quota-group-item">
        <div class="quota-group-header">
          <span class="quota-group-icon">${groupData.icon}</span>
          <div class="quota-group-info">
            <div class="quota-group-name">${escapeHtml(groupName)}</div>
            <div class="quota-group-models" data-collapsible="true">(${groupData.modelIds.map(id => escapeHtml(id)).join(', ')})</div>
            <div class="quota-group-description">${escapeHtml(groupData.description)}</div>
          </div>
        </div>
        <div class="quota-progress-bar">
          <div class="quota-progress-fill ${colorClass}" style="width: ${remainingPercentage}%"></div>
        </div>
        <div class="quota-group-stats">
          <span class="quota-percentage">${remainingPercentage}%</span>
          <span class="quota-reset-time">重置: ${resetTime}</span>
          <span class="quota-model-count">${groupData.modelIds.length} 个模型</span>
        </div>
      </div>
    `;
  }

  // 渲染其他模型
  if (otherModels.length > 0) {
    html += `
      <div class="quota-group-item quota-other-group">
        <div class="quota-group-header">
          <span class="quota-group-icon">📋</span>
          <div class="quota-group-info">
            <div class="quota-group-name">其他模型</div>
            <div class="quota-group-description">未分组模型单独计费</div>
          </div>
        </div>
        <div class="quota-other-models">
    `;

    otherModels.forEach(model => {
      const remainingPercentage = Math.round(model.remaining * 100);
      const colorClass = remainingPercentage > 50 ? 'quota-high' :
                        remainingPercentage > 20 ? 'quota-medium' : 'quota-low';

      html += `
        <div class="quota-single-model">
          <div class="quota-model-name">${escapeHtml(model.name)}</div>
          <div class="quota-progress-bar">
            <div class="quota-progress-fill ${colorClass}" style="width: ${remainingPercentage}%"></div>
          </div>
          <div class="quota-model-info">
            <span class="quota-percentage">${remainingPercentage}%</span>
            <span class="quota-reset-time">重置: ${model.resetTime}</span>
          </div>
        </div>
      `;
    });

    html += `
        </div>
      </div>
    `;
  }

  html += '</div>';
  container.innerHTML = html;

  // 绑定折叠按钮事件
  const toggleBtn = container.querySelector('.quota-toggle-btn');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', function() {
      const isCollapsed = this.getAttribute('data-collapsed') === 'true';
      const newState = !isCollapsed;

      // 更新状态
      this.setAttribute('data-collapsed', newState);
      container.querySelector('.quota-groups').setAttribute('data-collapsed', newState);

      // 更新按钮显示
      this.querySelector('.quota-toggle-icon').textContent = newState ? '▶' : '▼';
      this.querySelector('.quota-toggle-text').textContent = newState ? '展开模型' : '收起模型';

      // 保存到localStorage
      localStorage.setItem('quota-models-collapsed', newState);
    });
  }
}

async function refreshAccounts() {
  try {
    const [authData] = await Promise.all([
      fetchJson('/auth/accounts'),
      loadTokenRuntimeStats()
    ]);
    accountsData = authData.accounts || [];
    updateFilteredAccounts();
    loadHourlyUsage();
  } catch (e) {
    listEl.textContent = '加载失败: ' + e.message;
  }
}

function renderAccountsList() {
  if (!filteredAccounts.length) {
    listEl.innerHTML = accountsData.length
      ? '<div class="quota-placeholder">没有符合筛选条件的凭证。</div>'
      : '<div class="quota-placeholder">暂无账号，请先添加一个。</div>';
    if (paginationInfo) paginationInfo.textContent = '第 0 / 0 页';
    if (prevPageBtn) prevPageBtn.disabled = true;
    if (nextPageBtn) nextPageBtn.disabled = true;
    return;
  }

  const totalPages = Math.max(1, Math.ceil(filteredAccounts.length / PAGE_SIZE));
  currentPage = Math.min(Math.max(currentPage, 1), totalPages);
  const start = (currentPage - 1) * PAGE_SIZE;
  const pageItems = filteredAccounts.slice(start, start + PAGE_SIZE);

  listEl.innerHTML = pageItems.map(acc => {
    const displayName = escapeHtml(getAccountDisplayName(acc));
    const projectId = acc.projectId ? escapeHtml(acc.projectId) : '无 Project ID';
    const created = acc.createdAt ? new Date(acc.createdAt).toLocaleDateString() : '';

    // Status
    const statusClass = acc.enable ? 'cred-status-enabled' : 'cred-status-disabled';
    const statusText = acc.enable ? '启用' : '停用';

    // Runtime stats
    const stats = tokenRuntimeStats[acc.projectId] || { successCount: 0, failureCount: 0, inCooldown: false, lastFailure: 0 };
    const totalReqs = stats.successCount + stats.failureCount;
    const successRate = totalReqs > 0 ? Math.round((stats.successCount / totalReqs) * 100) : 100;
    const rateClass = totalReqs === 0 ? '' : successRate >= 80 ? 'cred-rate-high' : successRate >= 50 ? 'cred-rate-medium' : 'cred-rate-low';
    const ratePillClass = totalReqs === 0 ? '' : successRate >= 80 ? 'cred-stat-success' : successRate >= 50 ? 'cred-stat-warning' : 'cred-stat-danger';

    // Cooldown
    let cooldownHtml = '';
    if (stats.inCooldown) {
      const cooldownEnd = stats.lastFailure + tokenCooldownMs;
      const remainingSeconds = Math.max(0, Math.ceil((cooldownEnd - Date.now()) / 1000));
      cooldownHtml = `<span class="cred-cooldown">❄️ ${remainingSeconds}s</span>`;
    }

    return `
      <div class="cred-card" data-index="${acc.index}">
        <div class="cred-card-header">
          <div class="cred-card-info">
            <div class="cred-card-name" title="${displayName}">${displayName}</div>
            <div class="cred-card-project" title="${projectId}">${projectId}</div>
            ${created ? `<div class="cred-card-meta">创建: ${created}</div>` : ''}
          </div>
          <span class="cred-status-pill ${statusClass}">${statusText}</span>
        </div>

        <div class="cred-card-stats">
          <span class="cred-stat-pill ${ratePillClass}">
            ${totalReqs > 0 ? `成功率 ${successRate}%` : '暂无调用'}
          </span>
          ${cooldownHtml}
        </div>

        ${totalReqs > 0 ? `
        <div class="cred-rate-bar">
          <div class="cred-rate-fill ${rateClass}" style="width: ${successRate}%"></div>
        </div>
        ` : ''}

        <div class="cred-card-meta">
          ✅ ${stats.successCount} / ❌ ${stats.failureCount}
        </div>

        <div class="cred-card-actions">
          <button class="mini-btn primary" data-action="refresh" data-index="${acc.index}" title="刷新凭证" aria-label="刷新凭证">🔁</button>
          <button class="mini-btn" data-action="toggle" data-enable="${acc.enable}" data-index="${acc.index}" title="${acc.enable ? '停用' : '启用'}" aria-label="${acc.enable ? '停用' : '启用'}">
            ${acc.enable ? '⏸️' : '▶️'}
          </button>
          <button class="mini-btn" data-action="reauthorize" data-index="${acc.index}" title="重新授权" aria-label="重新授权">🔑</button>
          <button class="mini-btn" data-action="refreshProjectId" data-index="${acc.index}" title="刷新项目ID" aria-label="刷新项目ID">🔄</button>
          <button class="mini-btn" data-action="viewQuota" data-index="${acc.index}" data-name="${displayName}" title="查看额度" aria-label="查看额度">📊</button>
          <button class="mini-btn danger" data-action="delete" data-index="${acc.index}" title="删除" aria-label="删除">🗑️</button>
        </div>
      </div>
    `;
  }).join('');

  if (paginationInfo) {
    paginationInfo.textContent = `第 ${currentPage} / ${totalPages} 页，共 ${filteredAccounts.length} 个凭证`;
  }
  if (prevPageBtn) prevPageBtn.disabled = currentPage === 1;
  if (nextPageBtn) nextPageBtn.disabled = currentPage === totalPages;
}

async function deleteDisabledAccounts() {
  const disabledAccounts = accountsData
    .filter(acc => !acc.enable)
    .sort((a, b) => b.index - a.index);
  if (disabledAccounts.length === 0) {
    setStatus('没有停用的凭证需要删除。', 'info', manageStatusEl);
    return;
  }

  if (!confirm(`确认删除 ${disabledAccounts.length} 个停用凭证吗？删除后无法恢复。`)) return;

  deleteDisabledBtn.disabled = true;
  setStatus('正在删除停用凭证...', 'info', manageStatusEl);

  try {
    for (const acc of disabledAccounts) {
      await fetchJson(`/auth/accounts/${acc.index}`, { method: 'DELETE' });
    }
    setStatus(`已删除 ${disabledAccounts.length} 个停用凭证。`, 'success', manageStatusEl);
    await refreshAccounts();
  } catch (e) {
    setStatus('删除停用凭证失败: ' + e.message, 'error', manageStatusEl);
  } finally {
    deleteDisabledBtn.disabled = false;
  }
}

function renderSettings(groups) {
  if (!settingsGrid) return;
  if (!groups || groups.length === 0) {
    settingsGrid.textContent = '暂无配置数据';
    return;
  }

  const html = groups
    .map(group => {
      const items = (group.items || [])
        .map(item => {
          const currentValue = item?.value ?? '未设置';
          const editableValue = item.sensitive ? '' : currentValue;
          const defaultValue = item?.defaultValue ?? '无默认值';

          // 显示格式：如果设置了环境变量，显示"环境变量值 (默认值: 默认值)"
          const displayValue = item.isDefault
            ? (item.defaultValue !== null && item.defaultValue !== undefined ? defaultValue : currentValue)
            : `${currentValue} ${defaultValue !== '无默认值' ? `(默认值: ${defaultValue})` : ''}`;

          const badges = [
            `<span class="chip ${item.isDefault ? '' : item.source === 'docker' ? 'chip-warning' : item.source === 'env' ? 'chip-info' : 'chip-success'}">${
              item.isDefault ? '默认值' :
              item.source === 'docker' ? 'Docker环境变量' :
              item.source === 'env' ? '环境变量' :
              '配置文件'
            }</span>`,
            item.sensitive ? '<span class="chip chip-warning">敏感信息</span>' : '',
            item.dockerOnly ? '<span class="chip chip-warning">Docker专用</span>' : ''
          ]
            .filter(Boolean)
            .join('');

          const metaParts = [
            item.isDefault ? '使用默认值' :
              item.source === 'docker' ? '来自Docker环境变量' :
              item.source === 'env' ? '来自环境变量' :
              '来自data/config.json文件',
            `环境变量名: ${item.key}`,
            item.description ? escapeHtml(item.description) : ''
          ]
            .filter(Boolean)
            .join(' · ');

          return `
            <div class="setting-item ${item.isMissing ? 'missing' : ''}">
              <div class="setting-header">
                <div class="setting-key">${escapeHtml(item.label || item.key)}</div>
                ${badges}
              </div>
              <div class="setting-value">${escapeHtml(displayValue)}</div>
              <div class="setting-meta">${metaParts}</div>
              <div class="setting-actions">
                <button
                  class="mini-btn setting-edit-btn"
                  data-key="${escapeHtml(item.key)}"
                  data-label="${escapeHtml(item.label || item.key)}"
                  data-sensitive="${item.sensitive ? 'true' : 'false'}"
                  data-current="${escapeHtml(String(editableValue ?? ''))}"
                >
                  ✏️ 修改
                </button>
              </div>
            </div>
          `;
        })
        .join('');

      return `
        <div class="settings-group">
          <div class="settings-group-header">${escapeHtml(group.name || '配置')}</div>
          <div class="settings-list">${items || '<div class="setting-item">暂无配置</div>'}</div>
        </div>
      `;
    })
    .join('');

  settingsGrid.innerHTML = html;
}

async function loadSettings() {
  if (!settingsGrid) return;
  settingsGrid.textContent = '加载中...';
  try {
    const data = await fetchJson('/admin/settings');
    renderSettings(data.groups || []);
    if (data.updatedAt) {
      setStatus(`已更新：${new Date(data.updatedAt).toLocaleString()}`, 'success', settingsStatusEl);
    }
  } catch (e) {
    settingsGrid.textContent = '加载设置失败: ' + e.message;
    setStatus('刷新失败: ' + e.message, 'error', settingsStatusEl);
  }
}

async function updateSettingValue({ key, label, isSensitive, currentValue }) {
  if (!key) return;

  const promptMessage = [
    `${label || key} (${key})`,
    '留空可回退到默认值，更新后会立即保存到 data/config.json。',
    isSensitive ? '敏感信息不会显示当前值，请直接输入新值。' : null
  ]
    .filter(Boolean)
    .join('\n');

  const newValue = window.prompt(promptMessage, isSensitive ? '' : currentValue || '');
  if (newValue === null) return;

  try {
    setStatus('保存配置中...', 'info', settingsStatusEl);
    const response = await fetchJson('/admin/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ key, value: newValue })
    });

    if (response.dockerOnly) {
      // Docker专用配置的特殊提示
      setStatus(`此配置为 Docker 专用，请在 docker-compose.yml 的 environment 部分修改。`, 'warning', settingsStatusEl);
      alert(`⚠️ ${response.error}\n\n请在 docker-compose.yml 的 environment 部分修改此配置：\n${key}=你的值`);
    } else {
      await loadSettings();
      await loadTokenRuntimeStats();
      setStatus('已保存到 data/config.json。', 'success', settingsStatusEl);
    }
  } catch (e) {
    setStatus('更新失败: ' + e.message, 'error', settingsStatusEl);
  }
}

async function loadLogSettings() {
  if (!logLevelSelect) return;
  try {
    const data = await fetchJson('/admin/logs/settings');
    const raw = (data.level || 'all').toLowerCase();
    logLevelSelect.value = ['off', 'error', 'all'].includes(raw) ? raw : 'all';
  } catch (e) {
    console.error('加载调用日志配置失败:', e);
  }
}

function initLogSettingsUI() {
  const logsHeader = document.querySelector('[data-tab="logs"] .card-header');
  if (!logsHeader || !logsRefreshBtn) return;

  if (logLevelSelect) {
    loadLogSettings();
    return;
  }

  const actions = document.createElement('div');
  actions.className = 'card-actions';

  const label = document.createElement('label');
  label.className = 'setting-inline';
  label.style.display = 'flex';
  label.style.alignItems = 'center';
  label.style.gap = '8px';

  const span = document.createElement('span');
  span.textContent = '记录级别';
  span.title = '控制服务端记录哪些日志，不影响已有日志的显示';

  const select = document.createElement('select');
  select.className = 'input select';
  select.style.margin = '0';
  select.style.width = 'auto';
  select.style.fontSize = '12px';
  select.style.padding = '4px 24px 4px 8px';

  const options = [
    { value: 'all', text: '全部记录 (All)' },
    { value: 'error', text: '仅记录错误 (Error Only)' },
    { value: 'off', text: '关闭记录 (Off)' }
  ];

  options.forEach(opt => {
    const option = document.createElement('option');
    option.value = opt.value;
    option.textContent = opt.text;
    select.appendChild(option);
  });

  label.appendChild(span);
  label.appendChild(select);
  actions.appendChild(label);

  logsRefreshBtn.remove();
  actions.appendChild(logsRefreshBtn);
  if (logsClearBtn) {
    logsClearBtn.remove();
    actions.appendChild(logsClearBtn);
  }
  logsHeader.appendChild(actions);

  logLevelSelect = select;

  logLevelSelect.addEventListener('change', async () => {
    const level = logLevelSelect.value;
    try {
      setStatus('正在更新调用日志设置...', 'info', statusEl);
      await fetchJson('/admin/logs/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ level })
      });
      setStatus('调用日志设置已更新', 'success', statusEl);
      await loadLogs();
    } catch (e) {
      setStatus('更新调用日志设置失败: ' + e.message, 'error', statusEl);
    }
  });

  loadLogSettings();
}

async function loadLogs() {
  if (!logsEl) return;
  logsEl.textContent = '加载中...';
  if (logPaginationInfo) logPaginationInfo.textContent = '加载中...';
  if (logPrevPageBtn) logPrevPageBtn.disabled = true;
  if (logNextPageBtn) logNextPageBtn.disabled = true;
  try {
    const data = await fetchJson('/admin/logs?limit=200');
    logsData = data.logs || [];
    applyLogFilters();
  } catch (e) {
    logsEl.textContent = '加载日志失败: ' + e.message;
    if (logPaginationInfo) logPaginationInfo.textContent = '';
  }
}

async function fetchLogDetail(logId) {
  if (!logId) throw new Error('缺少日志 ID');
  if (logDetailCache.has(logId)) return logDetailCache.get(logId);
  const data = await fetchJson(`/admin/logs/${logId}`);
  const detail = data.log;
  logDetailCache.set(logId, detail);
  return detail;
}

function renderLogDetailContent(detail, container) {
  if (!container) return;
  if (!detail) {
    container.textContent = '未找到详情';
    return;
  }

  const requestSnapshot = detail.detail?.request;
  const responseSnapshot = detail.detail?.response;
  const rawBody = responseSnapshot?.rawBody;
  const modelAnswer =
    responseSnapshot?.modelOutput ||
    responseSnapshot?.body?.modelOutput ||
    responseSnapshot?.body?.text ||
    responseSnapshot?.body ||
    responseSnapshot;

  const isError = !detail.success;
  const errorSummary = isError ? { status: detail.status, message: detail.message } : null;

  const tabsHtml = `
    <div class="detail-tabs">
      <button class="detail-tab-btn active" data-tab="summary">摘要</button>
      <button class="detail-tab-btn" data-tab="output">模型输出</button>
      <button class="detail-tab-btn" data-tab="request">完整请求</button>
      <button class="detail-tab-btn" data-tab="response">完整响应</button>
    </div>
  `;

  const summaryContent = isError
    ? `
      <div class="log-detail-block">
        <h4 style="color:var(--status-off-text)">❌ 失败原因</h4>
        <pre>${formatJson(errorSummary)}</pre>
      </div>
      ${rawBody ? `
      <div class="log-detail-block">
        <h4>原始响应 (Raw Body)</h4>
        <pre style="max-height:300px">${escapeHtml(typeof rawBody === 'string' ? rawBody : JSON.stringify(rawBody, null, 2))}</pre>
      </div>` : ''}
    `
    : `
      <div class="log-detail-block">
        <h4 style="color:var(--status-ok-text)">✅ 调用成功</h4>
        <div class="setting-item">
          <div class="setting-meta">状态码: ${detail.status}</div>
          <div class="setting-meta">耗时: ${detail.durationMs}ms</div>
          <div class="setting-meta">模型: ${escapeHtml(detail.model)}</div>
        </div>
      </div>
    `;

  const panesHtml = `
    <div class="detail-tab-pane active" data-tab="summary">${summaryContent}</div>
    <div class="detail-tab-pane" data-tab="output"><pre>${formatJson(modelAnswer || '暂无模型回答')}</pre></div>
    <div class="detail-tab-pane" data-tab="request"><pre>${formatJson(requestSnapshot || '暂无请求数据')}</pre></div>
    <div class="detail-tab-pane" data-tab="response"><pre>${formatJson(responseSnapshot || '暂无响应数据')}</pre></div>
  `;

  container.innerHTML = tabsHtml + panesHtml;

  // 绑定 Tab 切换
  container.querySelectorAll('.detail-tab-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      const tab = btn.dataset.tab;

      container.querySelectorAll('.detail-tab-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');

      container.querySelectorAll('.detail-tab-pane').forEach(p => p.classList.remove('active'));
      container.querySelector(`.detail-tab-pane[data-tab="${tab}"]`)?.classList.add('active');
    });
  });
}

function bindLogDetailToggles() {
  // 绑定详情展开/收起
  document.querySelectorAll('.log-detail-toggle')?.forEach(btn => {
    btn.addEventListener('click', async (e) => {
      e.stopPropagation();
      const targetId = btn.dataset.detailTarget;
      const detailEl = document.getElementById(targetId);
      if (!detailEl) return;

      const isOpen = detailEl.classList.contains('open');
      if (isOpen) {
        detailEl.classList.remove('open');
        detailEl.style.display = 'none';
        btn.textContent = '查看详情';
        return;
      }

      detailEl.style.display = 'block';
      detailEl.innerHTML = '<div style="padding:10px;color:var(--muted)">加载中...</div>';
      btn.disabled = true;
      try {
        const detail = await fetchLogDetail(btn.dataset.logId);
        renderLogDetailContent(detail, detailEl);
        detailEl.classList.add('open');
        btn.textContent = '收起详情';
      } catch (e) {
        detailEl.innerHTML = `<div class="quota-error">加载失败: ${escapeHtml(e.message)}</div>`;
      } finally {
        btn.disabled = false;
      }
    });
  });

  // 绑定卡片折叠
  document.querySelectorAll('.log-group-header')?.forEach(header => {
    header.addEventListener('click', () => {
      const card = header.closest('.log-group-card');
      if (card) {
        card.classList.toggle('expanded');
      }
    });
  });
}

function initLogFilters() {
  const logsBody = document.querySelector('.logs-body');
  if (!logsBody || document.querySelector('.log-filter-bar')) return;

  const bar = document.createElement('div');
  bar.className = 'log-filter-bar';
  bar.innerHTML = `
    <div class="filter-top-row">
      <div class="filter-section">
        <span class="filter-label">类型</span>
        <div class="filter-chips" data-group="type">
          <button class="filter-chip active" data-val="all">全部</button>
          <button class="filter-chip" data-val="normal">普通</button>
          <button class="filter-chip" data-val="retry">重试</button>
        </div>
      </div>
      <div class="filter-section">
        <span class="filter-label">状态</span>
        <div class="filter-chips" data-group="status">
          <button class="filter-chip active" data-val="all">全部</button>
          <button class="filter-chip" data-val="success">成功</button>
          <button class="filter-chip" data-val="failed">失败</button>
        </div>
      </div>
      <div class="filter-section">
        <span class="filter-label">时间</span>
        <div class="filter-chips" data-group="time">
          <button class="filter-chip active" data-val="all">全部</button>
          <button class="filter-chip" data-val="1h">1h</button>
          <button class="filter-chip" data-val="6h">6h</button>
          <button class="filter-chip" data-val="24h">24h</button>
          <button class="filter-chip" data-val="7d">7d</button>
          <button class="filter-chip" data-val="custom">自定义</button>
        </div>
      </div>
    </div>
    <div class="filter-bottom-row" id="customDateRow" style="display:none">
       <input type="datetime-local" id="filterStart" class="date-input">
       <span class="date-sep">至</span>
       <input type="datetime-local" id="filterEnd" class="date-input">
    </div>
    <div class="filter-info-row">
       <div id="filterResultCount"></div>
       <div id="activeChainBadge" class="chain-badge" style="display:none">
          <span>链: <span id="chainCid"></span></span>
          <button id="clearChainBtn" class="mini-btn-icon" title="清除筛选">✕</button>
       </div>
    </div>
  `;

  const pagination = logsBody.querySelector('.logs-pagination');
  if (pagination) {
    logsBody.insertBefore(bar, pagination);
  } else {
    logsBody.prepend(bar);
  }

  bar.querySelectorAll('.filter-chips button').forEach(btn => {
    btn.addEventListener('click', (e) => {
       const group = e.target.closest('.filter-chips').dataset.group;
       const val = e.target.dataset.val;
       e.target.parentNode.querySelectorAll('.filter-chip').forEach(b => b.classList.remove('active'));
       e.target.classList.add('active');
       logFilters[group] = val;
       if (group === 'time') {
          document.getElementById('customDateRow').style.display = val === 'custom' ? 'flex' : 'none';
       }
       applyLogFilters();
    });
  });

  const onDateChange = () => {
     const d1 = document.getElementById('filterStart').value;
     const d2 = document.getElementById('filterEnd').value;
     logFilters.customStart = d1 ? new Date(d1).getTime() : null;
     logFilters.customEnd = d2 ? new Date(d2).getTime() : null;
     applyLogFilters();
  };
  document.getElementById('filterStart').addEventListener('change', onDateChange);
  document.getElementById('filterEnd').addEventListener('change', onDateChange);

  document.getElementById('clearChainBtn').addEventListener('click', () => {
      logFilters.correlationId = null;
      applyLogFilters();
  });
}

function applyLogFilters() {
  const now = Date.now();
  filteredLogs = logsData.filter(log => {
      if (logFilters.type === 'normal' && log.isRetry) return false;
      if (logFilters.type === 'retry' && !log.isRetry) return false;
      if (logFilters.status === 'success' && !log.success) return false;
      if (logFilters.status === 'failed' && log.success) return false;

      const t = log.timestamp ? new Date(log.timestamp).getTime() : 0;
      if (logFilters.time !== 'all' && logFilters.time !== 'custom') {
          let windowMs = 0;
          if (logFilters.time === '1h') windowMs = 3600 * 1000;
          if (logFilters.time === '6h') windowMs = 6 * 3600 * 1000;
          if (logFilters.time === '24h') windowMs = 24 * 3600 * 1000;
          if (logFilters.time === '7d') windowMs = 7 * 24 * 3600 * 1000;
          if (t < now - windowMs) return false;
      }
      if (logFilters.time === 'custom') {
          if (logFilters.customStart && t < logFilters.customStart) return false;
          if (logFilters.customEnd && t > logFilters.customEnd) return false;
      }
      if (logFilters.correlationId && log.correlationId !== logFilters.correlationId) return false;
      return true;
  });

  const countEl = document.getElementById('filterResultCount');
  if (countEl) countEl.textContent = `显示 ${filteredLogs.length} / ${logsData.length} 条`;

  const infoRow = document.querySelector('.filter-info-row');
  if (infoRow) infoRow.classList.add('show');

  const chainBadge = document.getElementById('activeChainBadge');
  if (chainBadge) {
      chainBadge.style.display = logFilters.correlationId ? 'inline-flex' : 'none';
      const cidSpan = document.getElementById('chainCid');
      if (cidSpan) cidSpan.textContent = logFilters.correlationId ? logFilters.correlationId.slice(0,8) : '';
  }

  logCurrentPage = 1;
  renderLogs();
}

function renderLogs() {
  if (!logsEl) return;

  if (!filteredLogs.length) {
    logsEl.textContent = logsData.length ? '没有符合筛选条件的日志' : '暂无调用日志';
    if (logPaginationInfo) logPaginationInfo.textContent = '第 0 / 0 页';
    if (logPrevPageBtn) logPrevPageBtn.disabled = true;
    if (logNextPageBtn) logNextPageBtn.disabled = true;
    return;
  }

  // 1. 使用 Map 按 correlationId 分组（修复非相邻日志无法分组的问题）
  const cidGroups = new Map();  // correlationId -> { items: [], firstIndex: number }
  const singles = [];           // 无 correlationId 的日志

  filteredLogs.forEach((log, index) => {
    const cid = log.correlationId;
    if (cid) {
      if (!cidGroups.has(cid)) {
        cidGroups.set(cid, { items: [], firstIndex: index });
      }
      cidGroups.get(cid).items.push(log);
    } else {
      singles.push({ type: 'single', log, sortIndex: index });
    }
  });

  // 2. 合并分组和单条日志，按首条日志的原始顺序排序
  const groupedData = [];
  cidGroups.forEach((group, cid) => {
    // 按时间顺序排列组内日志（旧的在前）
    group.items.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    groupedData.push({ type: 'group', id: cid, items: group.items, sortIndex: group.firstIndex });
  });
  singles.forEach(s => groupedData.push(s));

  // 按原始顺序排序（保持时间倒序展示）
  groupedData.sort((a, b) => a.sortIndex - b.sortIndex);

  // 2. 对分组后的数据进行分页
  const totalPages = Math.max(1, Math.ceil(groupedData.length / LOG_PAGE_SIZE));
  logCurrentPage = Math.min(Math.max(logCurrentPage, 1), totalPages);
  const start = (logCurrentPage - 1) * LOG_PAGE_SIZE;
  const pageGroups = groupedData.slice(start, start + LOG_PAGE_SIZE);

  // 渲染函数 helper
  const renderInnerContent = (log, uniqueId, isTimeline = false) => {
    const time = new Date(log.timestamp).toLocaleString();
    const isRetry = log.isRetry === true;
    const cid = log.correlationId;

    let typeLabel = '';
    if (log.success && isRetry) typeLabel = `<span class="chip chip-success">重试成功 #${log.retryCount}</span>`;
    else if (log.willRetry) typeLabel = `<span class="chip chip-warning">将重试 #${(log.retryCount||0)+1}</span>`;
    else if (isRetry) typeLabel = `<span class="chip chip-warning">重试失败 #${log.retryCount}</span>`;

    const cidHtml = (!isTimeline && cid)
      ? `<span class="log-cid action-cid" title="筛选此调用链" data-cid="${escapeHtml(cid)}">[${escapeHtml(cid.slice(0, 8))}]</span>`
      : '';

    const errorHint = (!log.success && log.message) ? `<div class="log-error-hint">原因：${escapeHtml(log.message)}</div>` : '';
    const detailBtn = (log.hasDetail && log.id)
      ? `<button class="mini-btn log-detail-toggle" data-log-id="${log.id}" data-detail-target="detail-${uniqueId}">查看详情</button>
         <div class="log-detail" id="detail-${uniqueId}"></div>`
      : '';

    return `
      <div class="log-content">
        <div class="log-time">${time} ${typeLabel} ${cidHtml}</div>
        <div class="log-meta">
          ${escapeHtml(log.model) || '未知'} | ${escapeHtml(log.projectId) || '未知'}
          ${log.tokenId ? `| Token:${escapeHtml(log.tokenId.slice(-6))}` : ''}
        </div>
        ${!isTimeline ? `<div class="log-meta">${escapeHtml(log.method) || '未知'} ${escapeHtml(log.path) || '未知'}</div>` : ''}
        <div class="log-meta">${log.status || (log.success?'成功':'失败')} | ${log.durationMs}ms</div>
        ${errorHint}
        ${detailBtn}
      </div>
      <div class="log-status">${log.success ? '成功' : '失败'}</div>
    `;
  };

  logsEl.innerHTML = pageGroups.map((group, idx) => {
    const uniqueIdBase = `${logCurrentPage}-${idx}`;

    // 单条日志或无 correlationId：普通渲染
    if (group.type === 'single') {
      const log = group.log;
      const cls = log.success ? 'log-success' : 'log-fail';
      return `<div class="log-item ${cls}">${renderInnerContent(log, uniqueIdBase, false)}</div>`;
    }

    // 分组日志：检查是否只有一条
    const items = group.items;
    if (items.length === 1) {
      const log = items[0];
      const cls = log.success ? 'log-success' : 'log-fail';
      return `<div class="log-item ${cls}">${renderInnerContent(log, uniqueIdBase, false)}</div>`;
    }

    // 多条日志（重试组）：卡片 + 时间轴渲染
    const first = items[0];
    const isFinalSuccess = items.some(l => l.success);
    const cls = isFinalSuccess ? 'log-success' : 'log-fail';

    const timelineHtml = items.map((log, subIdx) => {
      const markerCls = log.success ? 'success' : 'failed';
      const itemCls = log.success ? 'log-success' : 'log-fail';
      return `
        <div class="timeline-item">
          <div class="timeline-marker ${markerCls}"></div>
          <div class="timeline-content ${itemCls}">
            ${renderInnerContent(log, `${uniqueIdBase}-${subIdx}`, true)}
          </div>
        </div>
      `;
    }).join('');

    return `
      <div class="log-group-card ${cls === 'log-success' ? '' : 'expanded'}">
        <div class="log-group-header">
          <div class="log-group-title">
            <span class="group-toggle-icon">▶</span>
            <span>调用链 ${escapeHtml(group.id.slice(0, 8))}</span>
            <span class="badge ${isFinalSuccess ? 'badge-success' : 'badge-error'}">
              ${isFinalSuccess ? '最终成功' : '最终失败'}
            </span>
          </div>
          <div class="log-meta">
            ${items.length} 次尝试 · ${new Date(first.timestamp).toLocaleTimeString()}
          </div>
        </div>
        <div class="log-group-body">
           ${timelineHtml}
        </div>
      </div>
    `;
  }).join('');

  if (logPaginationInfo) {
    logPaginationInfo.textContent = `第 ${logCurrentPage} / ${totalPages} 页，共 ${groupedData.length} 条记录`;
  }
  if (logPrevPageBtn) logPrevPageBtn.disabled = logCurrentPage === 1;
  if (logNextPageBtn) logNextPageBtn.disabled = logCurrentPage === totalPages;

  bindLogDetailToggles();
  bindLogCorrelationHighlight();

  // 重新绑定 CID 点击
  logsEl.querySelectorAll('.action-cid').forEach(el => {
    el.addEventListener('click', (e) => {
      e.stopPropagation();
      logFilters.correlationId = e.target.dataset.cid;
      applyLogFilters();
    });
  });
}

function bindLogCorrelationHighlight() {
  const container = document.getElementById('logs');
  if (!container || container._correlationHandlerAttached) return;

  container.addEventListener('mouseover', e => {
    const item = e.target.closest('.log-item');
    if (!item) return;
    const cid = item.dataset.correlationId;
    if (!cid) return;

    container.querySelectorAll(`.log-item[data-correlation-id="${cid}"]`).forEach(el => {
      el.classList.add('log-related-active');
    });
  });

  container.addEventListener('mouseout', e => {
    const item = e.target.closest('.log-item');
    if (!item) return;
    const cid = item.dataset.correlationId;
    if (!cid) return;

    container.querySelectorAll(`.log-item[data-correlation-id="${cid}"]`).forEach(el => {
      el.classList.remove('log-related-active');
    });
  });

  container._correlationHandlerAttached = true;
}

async function loadHourlyUsage() {
  if (!hourlyUsageEl) return;
  hourlyUsageEl.textContent = '加载中...';
  try {
    const data = await fetchJson('/admin/logs/usage');
    const usageMap = new Map();
    (data.usage || []).forEach(item => {
      if (!item) return;
      usageMap.set(item.projectId || '未知项目', item);
    });

    const merged = (accountsData.length ? accountsData : Array.from(usageMap.values()))
      .map(acc => {
        const projectId = acc.projectId || acc.project || acc.id || '未知项目';
        const stats = usageMap.get(projectId) || {};

        // 使用 60 分钟窗口数据，保持一致性
        const hourlyCount = stats.count || 0;
        const hourlySuccess = stats.success || 0;
        const hourlyFailed = stats.failed || 0;
        const lastUsedAt = stats.lastUsedAt || null;

        const hasActivity = hourlyCount > 0 || !!lastUsedAt;

        return {
          projectId,
          index: acc.index,
          label: getAccountDisplayName(acc),
          count: hourlyCount,
          success: hourlySuccess,
          failed: hourlyFailed,
          lastUsedAt,
          hasActivity
        };
      })
      .filter(item => item.hasActivity);

    const windowMinutes = data.windowMinutes || HOUR_WINDOW_MINUTES;
    const limit = data.limitPerCredential || HOURLY_LIMIT;

    if (!merged.length) {
      hourlyUsageEl.textContent = '暂无最近 1 小时内的调用记录';
      return;
    }

    const sorted = merged.sort((a, b) => {
      const aTime = a.lastUsedAt ? Date.parse(a.lastUsedAt) : 0;
      const bTime = b.lastUsedAt ? Date.parse(b.lastUsedAt) : 0;
      if (aTime !== bTime) return bTime - aTime;
      return (b.count || 0) - (a.count || 0);
    });

    const html = sorted
      .map((item, mapIndex) => {
        const percent = Math.min(100, Math.round(((item.count || 0) / limit) * 100));
        const lastUsedText = item.lastUsedAt ? new Date(item.lastUsedAt).toLocaleString() : '暂无';
        // 使用 mapIndex 生成唯一 ID 避免 projectId 特殊字符或重复导致的问题
        const uniqueId = `quota-inline-${mapIndex}`;
        // 只有当 index 存在时才显示加载额度按钮
        const hasIndex = item.index !== undefined && item.index !== null;

        return `
          <div class="log-usage-row">
            <div class="log-usage-header">
              <div class="log-usage-title">${escapeHtml(item.label)}</div>
              <div class="log-usage-meta">${item.count || 0} / ${limit} 次 · ${windowMinutes} 分钟</div>
            </div>
            <div class="progress-bar" aria-label="${escapeHtml(item.label)} 用量">
              <div class="progress" style="width:${percent}%;"></div>
            </div>
            <div class="log-usage-stats">
              <div class="log-usage-stat">
                <span class="stat-label">成功 / 失败</span>
                <span class="stat-value">${item.success || 0} / ${item.failed || 0}</span>
              </div>
              <div class="log-usage-stat">
                <span class="stat-label">最近使用</span>
                <span class="stat-value">${escapeHtml(lastUsedText)}</span>
              </div>
              ${hasIndex ? `<button class="quota-toggle-btn mini-btn" data-target-id="${uniqueId}" data-index="${item.index}">📊 加载额度</button>` : ''}
            </div>
            <div class="quota-inline-container" id="${uniqueId}" style="display: none;"></div>
          </div>
        `;
      })
      .join('');

    hourlyUsageEl.innerHTML = html;

    // 绑定额度加载/收起按钮事件
    hourlyUsageEl.querySelectorAll('.quota-toggle-btn').forEach(btn => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const targetId = btn.dataset.targetId;
        const accountIndex = btn.dataset.index;
        const container = document.getElementById(targetId);
        if (!container) return;

        const isVisible = container.style.display !== 'none';
        if (isVisible) {
          // 收起
          container.style.display = 'none';
          btn.textContent = '📊 加载额度';
        } else {
          // 展开并加载
          container.style.display = 'block';
          btn.textContent = '⏳ 加载中...';
          btn.disabled = true;

          try {
            const data = await fetchJson(`/admin/tokens/${accountIndex}/quotas`, { cache: 'no-store' });
            renderQuota(container, data.data);
            btn.textContent = '📊 收起额度';
          } catch (err) {
            container.innerHTML = `<div class="quota-error">加载失败: ${escapeHtml(err.message)}</div>`;
            btn.textContent = '📊 重试加载';
          } finally {
            btn.disabled = false;
          }
        }
      });
    });
  } catch (e) {
    hourlyUsageEl.textContent = '加载用量失败: ' + e.message;
  }
}

if (loginBtn) {
  loginBtn.addEventListener('click', async () => {
    try {
      loginBtn.disabled = true;
      setStatus('获取授权链接中...', 'info');
      const data = await fetchJson('/auth/oauth/url');
      if (!data.url) throw new Error('未返回 url');
      setStatus('已打开授权页面，请完成 Google 授权，然后复制回调页面地址栏中的完整 URL，粘贴到下方输入框并提交。', 'info');
      window.open(data.url, '_blank', 'noopener');
    } catch (e) {
      setStatus('获取授权链接失败: ' + e.message, 'error');
    } finally {
      loginBtn.disabled = false;
    }
  });
}

if (submitCallbackBtn && callbackUrlInput) {
  submitCallbackBtn.addEventListener('click', async () => {
    const url = callbackUrlInput.value.trim();
    if (!url) {
      setStatus('请先粘贴包含 code 参数的完整回调 URL。', 'error');
      return;
    }

    const customProjectId = customProjectIdInput ? customProjectIdInput.value.trim() : '';

    try {
      submitCallbackBtn.disabled = true;
      setStatus('正在解析回调 URL 并交换 token...', 'info');
      await fetchJson('/auth/oauth/parse-url', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          url,
          replaceIndex,
          customProjectId,
          allowRandomProjectId: !!allowRandomProjectIdCheckbox?.checked
        })
      });

      setStatus('授权成功，账号已添加。', 'success');
      callbackUrlInput.value = '';
      if (customProjectIdInput) {
        customProjectIdInput.value = '';
      }
      replaceIndex = null;
      refreshAccounts();
    } catch (e) {
      setStatus('解析回调 URL 失败: ' + e.message, 'error');
    } finally {
      submitCallbackBtn.disabled = false;
    }
  });
}

if (importTomlBtn && tomlInput) {
  importTomlBtn.addEventListener('click', async () => {
    const content = tomlInput.value.trim();
    if (!content) {
      setStatus('请粘贴 TOML 凭证内容后再导入。', 'error', tomlStatusEl);
      return;
    }

    const replaceExisting = !!replaceExistingCheckbox?.checked;
    const filterDisabled = filterDisabledCheckbox ? !!filterDisabledCheckbox.checked : true;

    try {
      importTomlBtn.disabled = true;
      setStatus('正在导入 TOML 凭证...', 'info', tomlStatusEl);
      const result = await fetchJson('/auth/accounts/import-toml', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ toml: content, replaceExisting, filterDisabled })
      });

      const summary = `导入成功：有效 ${result.imported ?? 0} 条，跳过 ${result.skipped ?? 0} 条，总计 ${result.total ?? 0} 个账号。`;
      setStatus(summary, 'success', tomlStatusEl);
      tomlInput.value = '';
      refreshAccounts();
      loadLogs();
    } catch (e) {
      setStatus('导入失败: ' + e.message, 'error', tomlStatusEl);
    } finally {
      importTomlBtn.disabled = false;
    }
  });
}

tabButtons.forEach(btn => {
  btn.addEventListener('click', () => activateTab(btn.dataset.tabTarget));
});

if (deleteDisabledBtn) {
  deleteDisabledBtn.addEventListener('click', deleteDisabledAccounts);
}

if (prevPageBtn) {
  prevPageBtn.addEventListener('click', () => {
    currentPage = Math.max(1, currentPage - 1);
    renderAccountsList();
  });
}

if (nextPageBtn) {
  nextPageBtn.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(filteredAccounts.length / PAGE_SIZE));
    currentPage = Math.min(totalPages, currentPage + 1);
    renderAccountsList();
  });
}

if (logPrevPageBtn) {
  logPrevPageBtn.addEventListener('click', () => {
    logCurrentPage = Math.max(1, logCurrentPage - 1);
    renderLogs();
  });
}

if (logNextPageBtn) {
  logNextPageBtn.addEventListener('click', () => {
    const totalPages = Math.max(1, Math.ceil(filteredLogs.length / LOG_PAGE_SIZE));
    logCurrentPage = Math.min(totalPages, logCurrentPage + 1);
    renderLogs();
  });
}

if (themeToggleBtn) {
  themeToggleBtn.addEventListener('click', () => {
    const current = document.documentElement.getAttribute('data-theme') || 'light';
    const next = current === 'dark' ? 'light' : 'dark';
    if (autoThemeTimer) {
      clearInterval(autoThemeTimer);
      autoThemeTimer = null;
    }
    applyTheme(next);
  });
}

if (logoutBtn) {
  logoutBtn.addEventListener('click', async () => {
    try {
      logoutBtn.disabled = true;
      setStatus('正在退出登录...', 'info');
      await fetch('/admin/logout', {
        method: 'POST',
        headers: { Accept: 'application/json' },
        credentials: 'same-origin'
      });
      window.location.href = '/admin/login';
    } catch (e) {
      setStatus('退出录失败: ' + e.message, 'error');
      logoutBtn.disabled = false;
    }
  });
}

if (refreshBtn) {
  refreshBtn.addEventListener('click', () => {
    refreshAccounts();
    loadLogs();
    loadHourlyUsage();
  });
}

if (refreshAllBtn) {
  refreshAllBtn.addEventListener('click', () => {
    refreshAllAccountsBatch();
  });
}

if (logsRefreshBtn) {
  logsRefreshBtn.addEventListener('click', async () => {
    try {
      logsRefreshBtn.disabled = true;
      logsRefreshBtn.textContent = '刷新中...';
      await loadLogs();
    } finally {
      logsRefreshBtn.textContent = '🔄 刷新日志';
      logsRefreshBtn.disabled = false;
    }
  });
}

if (logsClearBtn) {
  logsClearBtn.addEventListener('click', async () => {
    if (!confirm('确认清空所有调用日志吗？该操作不可恢复。')) return;

    try {
      logsClearBtn.disabled = true;
      logsClearBtn.textContent = '清空中...';
      await fetchJson('/admin/logs/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      setStatus('调用日志已清空', 'success', statusEl);
      logsData = [];
      logCurrentPage = 1;
      renderLogs();
      await loadHourlyUsage();
    } catch (e) {
      setStatus('清空日志失败: ' + e.message, 'error', statusEl);
    } finally {
      logsClearBtn.textContent = '🗑 清空日志';
      logsClearBtn.disabled = false;
    }
  });
}

if (usageRefreshBtn) {
  usageRefreshBtn.addEventListener('click', async () => {
    try {
      usageRefreshBtn.disabled = true;
      usageRefreshBtn.textContent = '刷新中...';
      await loadHourlyUsage();
      setStatus('用量已刷新', 'success', usageStatusEl);
    } catch (e) {
      setStatus('刷新用量失败: ' + e.message, 'error', usageStatusEl);
    } finally {
      usageRefreshBtn.textContent = '🔄 刷新数据';
      usageRefreshBtn.disabled = false;
    }
  });
}

if (loadAllQuotasBtn) {
  loadAllQuotasBtn.addEventListener('click', loadAllQuotas);
}

async function loadAllQuotas() {
  if (!accountsData.length) return;

  const enabledAccounts = accountsData.filter(acc => acc.enable !== false);
  if (enabledAccounts.length === 0) return;

  if (loadAllQuotasBtn) {
    loadAllQuotasBtn.disabled = true;
    loadAllQuotasBtn.textContent = '加载中...';
  }

  const quotaResults = [];

  for (let i = 0; i < enabledAccounts.length; i++) {
    const acc = enabledAccounts[i];

    // 防御性检查：跳过无效 index
    if (acc.index === undefined || acc.index === null) continue;

    if (loadAllQuotasBtn) {
      loadAllQuotasBtn.textContent = `加载中 ${i + 1}/${enabledAccounts.length}`;
    }

    try {
      const data = await fetchJson(`/admin/tokens/${acc.index}/quotas`, { cache: 'no-store' });
      quotaResults.push({
        account: acc,
        quota: data.data,
        error: null
      });
    } catch (e) {
      quotaResults.push({
        account: acc,
        quota: null,
        error: e.message
      });
    }
  }

  if (loadAllQuotasBtn) {
    loadAllQuotasBtn.disabled = false;
    loadAllQuotasBtn.textContent = '📥 加载所有额度';
  }
}

if (settingsRefreshBtn) {
  settingsRefreshBtn.addEventListener('click', async () => {
    try {
      settingsRefreshBtn.disabled = true;
      settingsRefreshBtn.textContent = '刷新中...';
      await loadSettings();
    } finally {
      settingsRefreshBtn.textContent = '🔄 刷新配置';
      settingsRefreshBtn.disabled = false;
    }
  });
}

if (settingsGrid) {
  settingsGrid.addEventListener('click', async event => {
    const target = event.target.closest('.setting-edit-btn');
    if (!target) return;

    await updateSettingValue({
      key: target.dataset.key,
      label: target.dataset.label,
      isSensitive: target.dataset.sensitive === 'true',
      currentValue: target.dataset.current
    });
  });
}

refreshAccounts();
initLogFilters();
initLogSubTabs();
loadLogs();
loadHourlyUsage();
loadSettings();
initLogSettingsUI();

// === 新增：筛选标签事件委托 ===
const credStatusFilters = document.getElementById('credStatusFilters');
if (credStatusFilters) {
  credStatusFilters.addEventListener('click', (e) => {
    const tag = e.target.closest('.cred-filter-tag');
    if (!tag) return;

    // Update UI
    credStatusFilters.querySelectorAll('.cred-filter-tag').forEach(t => {
      t.classList.remove('active');
      t.setAttribute('aria-pressed', 'false');
    });
    tag.classList.add('active');
    tag.setAttribute('aria-pressed', 'true');

    // Update state
    const val = tag.dataset.value;
    if (val === 'error') {
      statusFilter = 'all';
      errorOnly = true;
    } else {
      statusFilter = val;
      errorOnly = false;
    }

    updateFilteredAccounts();
  });
}

// === 新增：凭证卡片操作事件委托 ===
if (listEl) {
  listEl.addEventListener('click', async (e) => {
    const btn = e.target.closest('[data-action]');
    if (!btn) return;

    const action = btn.dataset.action;
    const idx = btn.dataset.index;

    switch (action) {
      case 'refresh':
        btn.disabled = true;
        setStatus('正在刷新凭证...', 'info', manageStatusEl);
        try {
          await fetchJson(`/auth/accounts/${idx}/refresh`, { method: 'POST' });
          setStatus('刷新成功', 'success', manageStatusEl);
          refreshAccounts();
        } catch (err) {
          setStatus('刷新失败: ' + err.message, 'error', manageStatusEl);
        } finally {
          btn.disabled = false;
        }
        break;

      case 'toggle':
        const enable = btn.dataset.enable === 'false';
        btn.disabled = true;
        setStatus(enable ? '正在启用账号...' : '正在停用账号...', 'info', manageStatusEl);
        try {
          await fetchJson(`/auth/accounts/${idx}/enable`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ enable })
          });
          setStatus(enable ? '已启用账号' : '已停用账号', 'success', manageStatusEl);
          refreshAccounts();
        } catch (err) {
          setStatus('更新状态失败: ' + err.message, 'error', manageStatusEl);
        } finally {
          btn.disabled = false;
        }
        break;

      case 'delete':
        if (!confirm('确认删除这个账号吗？删除后无法恢复')) return;
        btn.disabled = true;
        setStatus('正在删除账号...', 'info', manageStatusEl);
        try {
          await fetchJson(`/auth/accounts/${idx}`, { method: 'DELETE' });
          setStatus('账号已删除', 'success', manageStatusEl);
          refreshAccounts();
        } catch (err) {
          setStatus('删除失败: ' + err.message, 'error', manageStatusEl);
        } finally {
          btn.disabled = false;
        }
        break;

      case 'reauthorize':
        replaceIndex = Number(idx);
        setStatus(`请重新授权账号 #${replaceIndex + 1}，完成后粘贴新的回调 URL 提交。`, 'info', manageStatusEl);
        loginBtn?.click();
        break;

      case 'refreshProjectId':
        btn.disabled = true;
        setStatus('正在刷新项目ID...', 'info', manageStatusEl);
        try {
          const res = await fetch('/auth/accounts/' + idx + '/refresh-project-id', {
            method: 'POST',
            credentials: 'same-origin',
            headers: { 'Content-Type': 'application/json' }
          });
          const data = await res.json().catch(() => ({}));
          if (!res.ok || data.error) throw new Error(data.error || `HTTP ${res.status}`);
          setStatus(`项目ID 已刷新为：${data.projectId || '未知'}`, 'success', manageStatusEl);
          await refreshAccounts();
        } catch (err) {
          setStatus('刷新项目ID失败: ' + err.message, 'error', manageStatusEl);
        } finally {
          btn.disabled = false;
        }
        break;

      case 'viewQuota':
        openQuotaModal(idx, btn.dataset.name || '凭证');
        break;
    }
  });
}

// === 新增：额度弹窗 ===
const quotaModal = document.getElementById('quotaModal');
const quotaModalTitle = document.getElementById('quotaModalTitle');
const quotaModalContent = document.getElementById('quotaModalContent');
const closeQuotaModalBtn = document.getElementById('closeQuotaModal');

async function openQuotaModal(accountIndex, accountName) {
  if (!quotaModal || !quotaModalContent) return;

  quotaModalTitle.textContent = `${accountName} - 额度详情`;
  quotaModalContent.innerHTML = '<div class="quota-loading">加载中...</div>';
  quotaModal.style.display = 'flex';

  try {
    const data = await fetchJson(`/admin/tokens/${accountIndex}/quotas`, { cache: 'no-store' });
    renderQuota(quotaModalContent, data.data);
  } catch (err) {
    quotaModalContent.innerHTML = `<div class="quota-error">加载失败: ${escapeHtml(err.message)}</div>`;
  }
}

function closeQuotaModalFn() {
  if (quotaModal) quotaModal.style.display = 'none';
}

if (closeQuotaModalBtn) {
  closeQuotaModalBtn.addEventListener('click', closeQuotaModalFn);
}

if (quotaModal) {
  quotaModal.addEventListener('click', (e) => {
    if (e.target === quotaModal) closeQuotaModalFn();
  });
}

// ========== 日志子标签页功能 ==========
let liveEventSource = null;
let isConsoleActive = false;
let dbStatsLoaded = false;

function initLogSubTabs() {
  const subTabBtns = document.querySelectorAll('.sub-tab-btn');
  const subTabContents = document.querySelectorAll('.sub-tab-content');

  subTabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.subtabTarget;

      // Update active state
      subTabBtns.forEach(b => b.classList.toggle('active', b === btn));
      subTabContents.forEach(c => c.classList.toggle('active', c.dataset.subtab === target));

      // Handle specific tab logic
      if (target === 'live') {
        startLiveConsole();
      } else {
        stopLiveConsole();
      }

      if (target === 'db') {
        loadDbStats();
      }
    });
  });

  // Init console controls
  const clearBtn = document.getElementById('clearConsoleBtn');
  const autoScrollChbox = document.getElementById('autoScrollConsole');
  if (clearBtn) clearBtn.addEventListener('click', () => {
    const term = document.getElementById('liveConsole');
    if (term) term.innerHTML = '';
  });
  if (autoScrollChbox) autoScrollChbox.addEventListener('change', (e) => {
    // console auto scroll state is read from checkbox directly
  });

  // Init DB controls
  document.getElementById('exportJsonBtn')?.addEventListener('click', () => window.open('/admin/logs/export?format=json'));
  document.getElementById('exportCsvBtn')?.addEventListener('click', () => window.open('/admin/logs/export?format=csv'));
  document.getElementById('cleanupDbBtn')?.addEventListener('click', handleDbCleanup);
}

// ========== 实时控制台功能 ==========
function startLiveConsole() {
  if (liveEventSource || isConsoleActive) return;
  isConsoleActive = true;

  const terminal = document.getElementById('liveConsole');
  const statusDot = document.querySelector('.status-dot');
  const statusText = document.getElementById('consoleStatusText');
  const autoScrollInfo = document.getElementById('autoScrollConsole');

  const appendLine = (text, type = 'info') => {
    if (!terminal) return;
    const div = document.createElement('div');
    div.className = `console-line ${type}`;
    const ts = new Date().toLocaleTimeString();
    div.innerHTML = `<span class="ts">[${ts}]</span>${escapeHtml(text)}`;
    terminal.appendChild(div);

    // Limit lines (max 500)
    if (terminal.childElementCount > 500) {
      terminal.removeChild(terminal.firstChild);
    }

    if (autoScrollInfo?.checked) {
      terminal.scrollTop = terminal.scrollHeight;
    }
  };

  try {
    liveEventSource = new EventSource('/admin/logs/live');

    liveEventSource.onopen = () => {
      if (statusDot) statusDot.className = 'status-dot connected';
      if (statusText) statusText.textContent = '已连接';
      appendLine('已连接到实时日志流...', 'system');
    };

    liveEventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'connected') return;

        if (data.type === 'log' && data.log) {
          const l = data.log;
          const msg = `${l.method} ${l.path} - ${l.status} (${l.durationMs}ms) ${l.model || ''}`;
          appendLine(msg, l.success ? 'info' : 'error');
        }
      } catch (e) {
        // ignore parse error
      }
    };

    liveEventSource.onerror = () => {
      if (statusDot) statusDot.className = 'status-dot disconnected';
      if (statusText) statusText.textContent = '连接断开，尝试重连...';
      liveEventSource.close();
      liveEventSource = null;
      isConsoleActive = false;
      // Auto reconnect after 2s if tab is still active
      setTimeout(() => {
        const activeTab = document.querySelector('.sub-tab-btn[data-subtab-target="live"]');
        if (activeTab && activeTab.classList.contains('active')) {
          startLiveConsole();
        }
      }, 2000);
    };
  } catch (e) {
    appendLine('无法建立连接: ' + e.message, 'error');
  }
}

function stopLiveConsole() {
  if (liveEventSource) {
    liveEventSource.close();
    liveEventSource = null;
  }
  isConsoleActive = false;
  const statusDot = document.querySelector('.status-dot');
  const statusText = document.getElementById('consoleStatusText');
  if (statusDot) statusDot.className = 'status-dot';
  if (statusText) statusText.textContent = '未连接';
}

// ========== 数据库管理功能 ==========
async function loadDbStats() {
  const grid = document.getElementById('dbStatsGrid');
  if (!grid) return;

  grid.innerHTML = '<div class="quota-loading">加载统计信息...</div>';

  try {
    const res = await fetchJson('/admin/logs/stats');
    if (!res.success) throw new Error('Failed to load stats');

    const s = res.stats || {};
    document.getElementById('retentionDays').textContent = s.retentionDays || 7;

    grid.innerHTML = `
      <div class="db-stat-card">
        <div class="db-stat-label">总日志数</div>
        <div class="db-stat-value">${s.totalLogs?.toLocaleString() || 0}</div>
      </div>
      <div class="db-stat-card">
        <div class="db-stat-label">数据库大小</div>
        <div class="db-stat-value">${s.dbSizeMB || 0} MB</div>
      </div>
      <div class="db-stat-card">
        <div class="db-stat-label">保留天数</div>
        <div class="db-stat-value">${s.retentionDays || 7} 天</div>
      </div>
      <div class="db-stat-card">
        <div class="db-stat-label">日志级别</div>
        <div class="db-stat-value" style="font-size:18px">${s.pipelineLogLevel || 'ALL'}</div>
      </div>
    `;

    // 同时加载日志文件列表
    loadLogFiles();
  } catch (e) {
    grid.innerHTML = `<div class="quota-error">加载失败: ${e.message}</div>`;
  }
}

async function handleDbCleanup() {
  if (!confirm('确定要手动清理过期日志吗？这将删除所有超过保留期限的日志。')) return;

  const btn = document.getElementById('cleanupDbBtn');
  const originalText = btn.textContent;
  btn.disabled = true;
  btn.textContent = '清理中...';

  try {
    const res = await fetchJson('/admin/logs/cleanup', { method: 'POST' });
    alert(`清理完成，共删除 ${res.deleted} 条旧日志。`);
    loadDbStats();
  } catch (e) {
    alert('清理失败: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.textContent = originalText;
  }
}

// ========== 日志文件列表功能 ==========

async function loadLogFiles() {
  const container = document.getElementById('logFilesContainer');
  if (!container) return;

  container.innerHTML = '<div class="quota-loading">加载文件列表...</div>';

  try {
    const { files = [] } = await fetchJson('/admin/logs/files');

    if (files.length === 0) {
      container.innerHTML = '<div class="quota-placeholder">暂无日志文件</div>';
      return;
    }

    container.innerHTML = `
      <table class="log-files-table">
        <thead>
          <tr>
            <th>文件名</th>
            <th>大小</th>
            <th>修改时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          ${files.map(f => `
            <tr>
              <td class="file-name">${escapeHtml(f.name)}</td>
              <td class="file-size">${escapeHtml(f.sizeFormatted)}</td>
              <td class="file-time">${escapeHtml(f.modifiedAtFormatted)}</td>
              <td class="file-actions">
                <button class="mini-btn" onclick="previewLogFile('${escapeHtml(f.name)}')">👁 预览</button>
                <button class="mini-btn" onclick="downloadLogFile('${escapeHtml(f.name)}')">📥 下载</button>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    `;
  } catch (e) {
    container.innerHTML = `<div class="quota-error">加载失败: ${escapeHtml(e.message)}</div>`;
  }
}

async function previewLogFile(filename) {
  const previewModal = document.getElementById('filePreviewModal');
  const previewTitle = document.getElementById('filePreviewTitle');
  const previewContent = document.getElementById('filePreviewContent');

  if (!previewModal || !previewContent) {
    alert('预览功能不可用');
    return;
  }

  previewTitle.textContent = filename;
  previewContent.textContent = '加载中...';
  previewModal.style.display = 'flex';

  try {
    const data = await fetchJson(`/admin/logs/files/${encodeURIComponent(filename)}`);

    if (data.type === 'database') {
      previewContent.textContent = data.content;
    } else {
      let content = data.content || '';
      if (data.truncated) {
        content += '\n\n... [文件过大，已截断显示，请下载查看完整内容]';
      }
      previewContent.textContent = content;
    }
  } catch (e) {
    previewContent.textContent = '加载失败: ' + e.message;
  }
}

function downloadLogFile(filename) {
  window.open(`/admin/logs/files/${encodeURIComponent(filename)}/download`, '_blank');
}

function closeFilePreviewModal() {
  const modal = document.getElementById('filePreviewModal');
  if (modal) modal.style.display = 'none';
}

// ========== 冻结历史功能 ==========
const freezeHistoryBtn = document.getElementById('freezeHistoryBtn');
const freezeHistoryModal = document.getElementById('freezeHistoryModal');
const closeFreezeHistoryModal = document.getElementById('closeFreezeHistoryModal');
const freezeHistoryList = document.getElementById('freezeHistoryList');

async function loadFreezeHistory() {
  if (!freezeHistoryList) return;
  
  freezeHistoryList.innerHTML = '<p class="loading">加载中...</p>';
  
  try {
    const { history = [] } = await fetchJson('/auth/accounts/freeze-history');
    
    if (history.length === 0) {
      freezeHistoryList.innerHTML = '<p class="freeze-history-empty">暂无冻结记录</p>';
      return;
    }
    
    freezeHistoryList.innerHTML = history.map(item => {
      const freezeTime = new Date(item.freezeTime).toLocaleString();
      const unfreezeTime = new Date(item.unfreezeTime).toLocaleString();
      const cooldownSeconds = Math.round(item.cooldownMs / 1000);
      
      return '<div class="freeze-history-item">' +
        '<div class="credential-id">' + escapeHtml(item.credentialId) + '</div>' +
        '<div class="reason">' + escapeHtml(item.reason) + '</div>' +
        '<div class="time-info">' +
          '<span>冻结时间: ' + freezeTime + '</span>' +
          '<span>解冻时间: ' + unfreezeTime + '</span>' +
          '<span>冷却: ' + cooldownSeconds + 's</span>' +
        '</div>' +
      '</div>';
    }).join('');
  } catch (e) {
    freezeHistoryList.innerHTML = '<p class="freeze-history-empty">加载失败: ' + escapeHtml(e.message) + '</p>';
  }
}

function openFreezeHistoryModal() {
  if (freezeHistoryModal) {
    freezeHistoryModal.style.display = 'flex';
    loadFreezeHistory();
  }
}

function closeFreezeHistoryModalFn() {
  if (freezeHistoryModal) {
    freezeHistoryModal.style.display = 'none';
  }
}

if (freezeHistoryBtn) {
  freezeHistoryBtn.addEventListener('click', openFreezeHistoryModal);
}

if (closeFreezeHistoryModal) {
  closeFreezeHistoryModal.addEventListener('click', closeFreezeHistoryModalFn);
}

if (freezeHistoryModal) {
  freezeHistoryModal.addEventListener('click', function(e) {
    if (e.target === freezeHistoryModal) {
      closeFreezeHistoryModalFn();
    }
  });
}
