# 转换器模块新手学习指南

> 帮助新手快速熟悉 API 格式转换代码

---

## 一、模块架构概览

### 目录结构

```
src/utils/converters/
├── index.js                    # 统一导出入口
├── openaiAdapter.js            # OpenAI → Gemini
├── anthropicAdapter.js         # Claude → Gemini
├── openaiToClaudeAdapter.js    # OpenAI ⇄ Claude
├── claudeToOpenaiAdapter.js    # Claude → OpenAI
└── common/                     # 公共模块
    ├── BaseSseEmitter.js       # SSE Emitter 基类
    ├── ClaudeProtocolEmitter.js    # Claude SSE 协议
    ├── OpenAIProtocolEmitter.js    # OpenAI SSE 协议
    ├── contentConverter.js     # 内容块转换
    └── toolConverter.js        # 工具定义转换
```

### 6 个转换方向

| 方向 | 适配器文件 | 主入口函数 |
|------|-----------|-----------|
| OpenAI → Gemini | `openaiAdapter.js` | `generateRequestBody()` |
| Gemini → OpenAI | `openaiAdapter.js` | `handleStreamResponse()` |
| Claude → Gemini | `anthropicAdapter.js` | `generateRequestBodyFromAnthropic()` |
| Gemini → Claude | `anthropicAdapter.js` | `handleAnthropicStreamResponse()` |
| OpenAI → Claude | `openaiToClaudeAdapter.js` | `mapOpenAIToClaude()` |
| Claude → OpenAI | `claudeToOpenaiAdapter.js` | `mapClaudeToOpenAI()` |

---

## 二、学习路径（3 阶段）

### 阶段 1：鸟瞰全局

**目标**：理解模块职责和调用关系

1. 阅读 `index.js`，了解导出了哪些函数
2. 查看每个适配器文件的**文件头注释**，了解转换方向
3. 搜索 `【请求转换 · 主入口】` 和 `【响应转换 · 主入口】` 定位核心函数

### 阶段 2：深入一条链路

**目标**：完整理解一个转换流程

选择一条路线深入：

#### 路线 A：OpenAI → Gemini（推荐新手）

```
openaiAdapter.js
├── 【请求转换】generateRequestBody()
│   ├── 消息格式转换
│   ├── 工具定义转换 → ToolConverter.toGemini()
│   └── 系统指令处理
└── 【响应转换】handleStreamResponse()
    ├── SSE 解析
    ├── 内容转换 → ContentConverter.geminiToOpenAI()
    └── OpenAIProtocolEmitter 输出
```

#### 路线 B：OpenAI ⇄ Claude

```
openaiToClaudeAdapter.js
├── 【请求转换】mapOpenAIToClaude()
│   ├── 角色映射 (system → 独立字段)
│   ├── 内容块转换 → ContentConverter.openaiToClaude()
│   └── 工具转换 → ToolConverter.toClaude()
└── 【响应转换】流式处理
    └── ClaudeProtocolEmitter 输出
```

### 阶段 3：掌握公共模块

**目标**：理解可复用组件

| 模块 | 职责 | 关键方法 |
|------|------|----------|
| `contentConverter.js` | 内容块格式互转 | `geminiToOpenAI()`, `openaiToClaude()` 等 |
| `toolConverter.js` | 工具定义格式互转 | `toGemini()`, `toOpenAI()`, `toClaude()` |
| `BaseSseEmitter.js` | SSE 基类 | `trackTokens()`, `buildUsage()` |
| `ClaudeProtocolEmitter.js` | Claude SSE 协议 | `sendText()`, `sendThinking()`, `sendToolCalls()` |
| `OpenAIProtocolEmitter.js` | OpenAI SSE 协议 | `sendText()`, `sendToolCallStart()`, `finish()` |

---

## 三、关键概念 Q&A

### Q1: 请求转换 vs 响应转换？

- **请求转换**：客户端请求进来 → 转成目标 API 格式 → 发给后端
- **响应转换**：后端响应回来 → 转成客户端期望格式 → 返回给客户端

代码中通过注释标记区分：
```javascript
// ==================== 【请求转换】OpenAI → Claude ====================
// ==================== 【响应转换】Claude → OpenAI ====================
```

### Q2: 流式 vs 非流��？

- **非流式**：一次性返回完整响应，用普通函数处理
- **流式**：SSE 逐块返回，需要 Emitter 类处理

标记方式：
```javascript
// 【响应转换 · 非流式】
// 【响应转换 · 流式】
```

### Q3: SSE 协议差异？

| 协议 | 特点 | Emitter 类 |
|------|------|-----------|
| Claude | event + data 格式，有 Block 生命周期 | `ClaudeProtocolEmitter` |
| OpenAI | 纯 data 格式，扁平流 | `OpenAIProtocolEmitter` |

### Q4: 内容块类型有哪些？

| 类型 | Claude 格式 | OpenAI 格式 |
|------|------------|-------------|
| 文本 | `{ type: 'text', text }` | `{ type: 'text', text }` |
| 图片 | `{ type: 'image', source: {...} }` | `{ type: 'image_url', image_url: {...} }` |
| 文档 | `{ type: 'document', source: {...} }` | `{ type: 'file', file: {...} }` |
| 思考 | `{ type: 'thinking', thinking }` | 不支持（忽略） |
| 工具调用 | `{ type: 'tool_use', id, name, input }` | `{ type: 'function', function: {...} }` |

### Q5: Emitter 继承关系？

```
BaseSseEmitter (基类)
├── ClaudeProtocolEmitter (Claude SSE 协议)
└── OpenAIProtocolEmitter (OpenAI SSE 协议)
```

> **注意**：`common/` 目录下的 Emitter 是新架构。部分适配器（如 `anthropicAdapter.js`）仍使用独立的 `ClaudeSseEmitter` 实现，这是重构过渡期的正常状态。

---

## 四、实战任务

### 任务 1：追踪一个请求

1. 在 `openaiAdapter.js` 找到 `generateRequestBody()`
2. 跟踪 `messages` 参数如何被转换成 Gemini 的 `contents`
3. 找出工具定义是如何调用 `ToolConverter.toGemini()` 的

### 任务 2：理解 SSE 输出

1. 阅读 `OpenAIProtocolEmitter.js` 的 `sendText()` 方法
2. 理解 `data: {...}\n\n` 格式是如何构造的
3. 对比 `ClaudeProtocolEmitter.js` 的 `sendText()` 有何不同

### 任务 3：添加新转换

假设需要支持新的内容类型 `audio`：
1. 在 `contentConverter.js` 找到内容转换函数
2. 分析现有类型（text, image, document）的处理模式
3. 思考如何添加 audio 类型的转换逻辑

---

## 五、快速查阅表

| 我想找... | 搜索关键词 |
|-----------|-----------|
| 请求转换主入口 | `【请求转换 · 主入口】` |
| 响应转换主入口 | `【响应转换 · 主入口】` |
| 流式响应处理 | `【响应转换 · 流式】` |
| 工具定义转换 | `ToolConverter` |
| 内容块转换 | `ContentConverter` |
| Claude SSE 输出 | `ClaudeProtocolEmitter` |
| OpenAI SSE 输出 | `OpenAIProtocolEmitter` |

---

## 六、调试技巧

1. **日志追踪**：在转换函数入口添加 `console.log` 打印输入输出
2. **断点调试**：在 VSCode 中对 Emitter 的 `sendText()` 设断点，观察流式输出
3. **对比工具**：用 diff 工具对比转换前后的 JSON 结构

---

*最后更新：2025-12-30*
