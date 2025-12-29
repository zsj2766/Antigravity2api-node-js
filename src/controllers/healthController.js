/**
 * 健康检查控制器 (Health Controller)
 *
 * 职责：
 * - 提供服务健康状态检查端点
 * - 用于 Docker 容器探活和监控系统
 * - 返回服务运行时间和时间信息
 *
 * @module controllers/healthController
 */

/**
 * 健康检查处理器
 *
 * 返回服务的基本健康状态信息，包括：
 * - status: 服务状态（'ok' 表示正常）
 * - uptime: 进程运行时间（秒）
 * - serverTime: 服务器时间（UTC ISO 格式）
 * - chinaTime: 中国时间（UTC+8 ISO 格式）
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 *
 * @example
 * // GET /healthz
 * // Response: { "status": "ok", "uptime": 3600, ... }
 */
export function healthCheck(req, res) {
  const now = new Date();
  const serverTime = now.toISOString();

  // 计算中国时间 (UTC+8)
  const deltaMinutes = 8 * 60 + now.getTimezoneOffset();
  const chinaDate = new Date(now.getTime() + deltaMinutes * 60000);
  const chinaTime = chinaDate.toISOString();

  res.json({
    status: 'ok',
    uptime: process.uptime(),
    serverTime,
    chinaTime
  });
}

/**
 * 详细健康检查处理器
 *
 * 返回更详细的健康状态信息，包括内存使用情况。
 * 适用于需要更深入监控的场景。
 *
 * @param {import('express').Request} req - Express 请求对象
 * @param {import('express').Response} res - Express 响应对象
 */
export function healthCheckDetailed(req, res) {
  const now = new Date();
  const memUsage = process.memoryUsage();

  res.json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: now.toISOString(),
    memory: {
      heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
      heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024) + 'MB',
      rss: Math.round(memUsage.rss / 1024 / 1024) + 'MB'
    },
    nodeVersion: process.version
  });
}

export default {
  healthCheck,
  healthCheckDetailed
};
