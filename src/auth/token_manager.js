import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import axios from 'axios';
import { log } from '../utils/logger.js';
import { generateProjectId, generateSessionId } from '../utils/idGenerator.js';
import config from '../config/config.js';
import { getUsageCountSince, getRecentTokenStats } from '../utils/log_store.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CLIENT_ID = '1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com';
const CLIENT_SECRET = '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';

class TokenManager {
  constructor(filePath = path.join(__dirname,'..','..','data' ,'accounts.json')) {
    this.filePath = filePath;
    this.tokens = [];
    this.currentIndex = 0;
    this.tokenStats = new Map(); // 凭证统计

    // 负载均衡状态
    this.stickyTokenId = null;
    this.stickyUsageCount = 0;

    // 用量缓存配置
    this.usageCache = new Map();
    this.USAGE_CACHE_TTL = 10000; // 10秒缓存

    this.updateConfig();
    this.initialize();
  }

  updateConfig() {
    this.hourlyLimit = Number.isFinite(Number(config.credentials?.maxUsagePerHour))
      ? Number(config.credentials.maxUsagePerHour)
      : 20;
    this.cooldownMs = Number.isFinite(Number(config.credentials?.cooldownMs))
      ? Number(config.credentials.cooldownMs)
      : 5 * 60 * 1000;
    this.MAX_STICKY_USAGE = Number.isFinite(Number(config.credentials?.maxStickyUsage))
      ? Number(config.credentials.maxStickyUsage)
      : 5;
    this.POOL_SIZE = Number.isFinite(Number(config.credentials?.poolSize))
      ? Number(config.credentials.poolSize)
      : 3;
  }

  getTokenKey(token) {
    return token.projectId || token.access_token;
  }

  getStats(token) {
    const key = this.getTokenKey(token);
    if (!this.tokenStats.has(key)) {
      this.tokenStats.set(key, { lastUsed: 0, lastFailure: 0, failureCount: 0, successCount: 0 });
    }
    return this.tokenStats.get(key);
  }

  recordSuccess(token) {
    const stats = this.getStats(token);
    stats.lastUsed = Date.now();
    stats.successCount += 1;
    stats.failureCount = 0;
  }

  recordFailure(token, statusCode) {
    const stats = this.getStats(token);
    stats.lastUsed = Date.now();
    stats.failureCount += 1;
    if (statusCode === 429) {
      stats.lastFailure = Date.now();
    }

    // 失败时重置粘性会话
    const key = this.getTokenKey(token);
    if (this.stickyTokenId === key) {
      this.stickyTokenId = null;
      this.stickyUsageCount = 0;
    }
  }

  isInCooldown(token) {
    const stats = this.getStats(token);
    if (stats.lastFailure === 0) return false;
    return Date.now() - stats.lastFailure < this.cooldownMs;
  }

  async prepareToken(token) {
    try {
      if (this.isExpired(token)) {
        await this.refreshToken(token);
      }

      if (!token.projectId) {
        if (config.skipProjectIdFetch) {
          token.projectId = generateProjectId();
          this.saveToFile();
        } else {
          const projectId = await this.fetchProjectId(token);
          if (projectId === undefined) {
            this.disableToken(token);
            return null;
          }
          token.projectId = projectId;
          this.saveToFile();
        }
      }

      return token;
    } catch (error) {
      if (error.statusCode === 403 || error.statusCode === 400) {
        this.disableToken(token);
        return null;
      }
      throw error;
    }
  }

  ensureDataFile() {
    const dir = path.dirname(this.filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(this.filePath)) {
      fs.writeFileSync(this.filePath, '[]', 'utf8');
      log.warn(`未找到账号文件，已创建空文件: ${this.filePath}`);
    }
  }

  setHourlyLimit(limit) {
    if (!Number.isFinite(Number(limit))) return;
    this.hourlyLimit = Number(limit);
  }

  reloadConfig() {
    log.info('TokenManager: reloading config...');
    const oldSticky = this.MAX_STICKY_USAGE;
    this.updateConfig();
    log.info(`TokenManager: config reloaded. MAX_STICKY_USAGE: ${oldSticky} -> ${this.MAX_STICKY_USAGE}`);
  }

  isWithinHourlyLimit(token) {
    if (!this.hourlyLimit || Number.isNaN(this.hourlyLimit)) return true;

    const now = Date.now();
    const cacheKey = token.projectId;
    let usage;

    // 检查缓存
    const cached = this.usageCache.get(cacheKey);
    if (cached && (now - cached.timestamp < this.USAGE_CACHE_TTL)) {
      usage = cached.count;
    } else {
      const oneHourAgo = now - 60 * 60 * 1000;
      usage = getUsageCountSince(token.projectId, oneHourAgo);
      this.usageCache.set(cacheKey, { count: usage, timestamp: now });
    }

    if (usage >= this.hourlyLimit) {
      log.warn(
        `账号 ${token.projectId || '未知'} 已达到每小时 ${this.hourlyLimit} 次上限，切换下一个账号`
      );
      return false;
    }

    return true;
  }

  moveToNextToken() {
    if (this.tokens.length === 0) {
      this.currentIndex = 0;
      return;
    }
    this.currentIndex = (this.currentIndex + 1) % this.tokens.length;
  }

  initialize() {
    try {
      log.info('正在初始化token管理器...');
      this.ensureDataFile();

      const data = fs.readFileSync(this.filePath, 'utf8');
      const parsed = JSON.parse(data || '[]');
      const tokenArray = Array.isArray(parsed) ? parsed : [];

      this.tokens = tokenArray.filter(token => token.enable !== false).map(token => ({
        ...token,
        sessionId: generateSessionId()
      }));

      // 从日志恢复运行时统计数据 (Fix Issue 1: 服务重启后历史统计丢失)
      try {
        const restoredStats = getRecentTokenStats();
        let restoredCount = 0;
        Object.entries(restoredStats).forEach(([key, stats]) => {
          this.tokenStats.set(key, stats);
          restoredCount++;
        });
        if (restoredCount > 0) {
          log.info(`已从历史日志恢复 ${restoredCount} 个凭证的运行时统计状态`);
        }
      } catch (statsError) {
        log.warn('从日志恢复统计状态失败:', statsError.message);
      }

      this.currentIndex = 0;
      log.info(`成功加载 ${this.tokens.length} 个可用token`);
    } catch (error) {
      log.error('初始化token失败:', error.message);
      this.tokens = [];
    }
  }

  async fetchProjectId(token) {
    const response = await axios({
      method: 'POST',
      url: 'https://daily-cloudcode-pa.sandbox.googleapis.com/v1internal:loadCodeAssist',
      headers: {
        'Host': 'daily-cloudcode-pa.sandbox.googleapis.com',
        'User-Agent': 'antigravity/1.11.9 windows/amd64',
        'Authorization': `Bearer ${token.access_token}`,
        'Content-Type': 'application/json',
        'Accept-Encoding': 'gzip'
      },
      data: JSON.stringify({ metadata: { ideType: 'ANTIGRAVITY' } }),
      timeout: config.timeout,
      proxy: config.proxy ? (() => {
        const proxyUrl = new URL(config.proxy);
        return { protocol: proxyUrl.protocol.replace(':', ''), host: proxyUrl.hostname, port: parseInt(proxyUrl.port) };
      })() : false
    });
    return response.data?.cloudaicompanionProject;
  }

  isExpired(token) {
    if (!token.timestamp || !token.expires_in) return true;
    const expiresAt = token.timestamp + (token.expires_in * 1000);
    return Date.now() >= expiresAt - 300000;
  }

  async refreshToken(token) {
    log.info('正在刷新token...');
    const body = new URLSearchParams({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: 'refresh_token',
      refresh_token: token.refresh_token
    });

    try {
      const response = await axios({
        method: 'POST',
        url: 'https://oauth2.googleapis.com/token',
        headers: {
          'Host': 'oauth2.googleapis.com',
          'User-Agent': 'Go-http-client/1.1',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept-Encoding': 'gzip'
        },
        data: body.toString(),
        timeout: config.timeout,
        proxy: config.proxy ? (() => {
          const proxyUrl = new URL(config.proxy);
          return { protocol: proxyUrl.protocol.replace(':', ''), host: proxyUrl.hostname, port: parseInt(proxyUrl.port) };
        })() : false
      });

      token.access_token = response.data.access_token;
      token.expires_in = response.data.expires_in;
      token.timestamp = Date.now();
      this.saveToFile();
      return token;
    } catch (error) {
      throw { statusCode: error.response?.status, message: error.response?.data || error.message };
    }
  }

  saveToFile() {
    try {
      this.ensureDataFile();
      const data = fs.readFileSync(this.filePath, 'utf8');
      const allTokens = JSON.parse(data);

      this.tokens.forEach(memToken => {
        const index = allTokens.findIndex(t => t.refresh_token === memToken.refresh_token);
        if (index !== -1) {
          const { sessionId, ...tokenToSave } = memToken;
          allTokens[index] = tokenToSave;
        }
      });

      fs.writeFileSync(this.filePath, JSON.stringify(allTokens, null, 2), 'utf8');
    } catch (error) {
      log.error('保存文件失败:', error.message);
    }
  }

  disableToken(token) {
    log.warn(`禁用token ...${token.access_token.slice(-8)}`)
    token.enable = false;
    this.saveToFile();
    this.tokens = this.tokens.filter(t => t.refresh_token !== token.refresh_token);
    this.currentIndex = this.currentIndex % Math.max(this.tokens.length, 1);
  }

  async getToken(excludeIds = new Set()) {
    if (this.tokens.length === 0) return null;

    // 1. 过滤可用凭证 (Enabled + Not Cooldown + Within Limit + Not Excluded)
    const usableTokens = this.tokens.filter(t =>
      t.enable !== false &&
      !this.isInCooldown(t) &&
      this.isWithinHourlyLimit(t) &&
      !excludeIds.has(this.getTokenKey(t))
    );

    // 兜底：如果没有完全符合条件的凭证，尝试使用仅受限但未禁用且未排除的凭证
    if (usableTokens.length === 0) {
      const fallbackTokens = this.tokens.filter(t =>
        t.enable !== false && !excludeIds.has(this.getTokenKey(t))
      );
      if (fallbackTokens.length === 0) return null;

      // 兜底策略：按 LRU 排序（优先使用最久未使用的）
      log.warn('所有凭证均已冷却或超限，使用兜底策略选择最久未使用的凭证');
      fallbackTokens.sort((a, b) => this.getStats(a).lastUsed - this.getStats(b).lastUsed);
      return this.prepareToken(fallbackTokens[0]);
    }

    // 2. 连续调用保护 (Sticky Session)
    // 只有当 stickyToken 未被排除时才使用
    if (this.stickyTokenId && this.stickyUsageCount < this.MAX_STICKY_USAGE) {
      if (!excludeIds.has(this.stickyTokenId)) {
        const stickyToken = usableTokens.find(t => this.getTokenKey(t) === this.stickyTokenId);
        if (stickyToken) {
          this.stickyUsageCount++;
          return this.prepareToken(stickyToken);
        }
      }
    }

    // 3. 负载均衡策略 (LRU + Weighted Random)

    // 按最后使用时间排序 (LRU)，最久未使用的在前
    usableTokens.sort((a, b) => this.getStats(a).lastUsed - this.getStats(b).lastUsed);

    // 选取候选池 (Top N)
    const poolSize = Math.min(usableTokens.length, this.POOL_SIZE);
    const candidates = usableTokens.slice(0, poolSize);

    // 加权随机选择 (权重 = 空闲时间)
    const now = Date.now();
    const candidatesWithWeights = candidates.map(token => {
      const stats = this.getStats(token);
      const idleTime = Math.max(0, now - stats.lastUsed);
      // 基础权重 1000ms，避免刚使用过的权重为 0
      return { token, weight: idleTime + 1000 };
    });

    const totalWeight = candidatesWithWeights.reduce((sum, item) => sum + item.weight, 0);
    let randomValue = Math.random() * totalWeight;
    let selectedToken = candidatesWithWeights[0].token;

    for (const item of candidatesWithWeights) {
      randomValue -= item.weight;
      if (randomValue <= 0) {
        selectedToken = item.token;
        break;
      }
    }

    // 更新粘性会话状态
    this.stickyTokenId = this.getTokenKey(selectedToken);
    this.stickyUsageCount = 1;

    // 预增缓存计数，防止高并发下多个请求在缓存刷新间隔内突破限额
    const cacheKey = selectedToken.projectId;
    if (cacheKey) {
      const cached = this.usageCache.get(cacheKey);
      if (cached) {
        cached.count += 1;
        this.usageCache.set(cacheKey, cached);
      }
    }

    return this.prepareToken(selectedToken);
  }

  disableCurrentToken(token) {
    const found = this.tokens.find(t => t.access_token === token.access_token);
    if (found) {
      this.disableToken(found);
    }
  }
}
const tokenManager = new TokenManager();
export default tokenManager;
