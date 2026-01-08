/**
 * Bridge 内部 JSON Schema 清理工具
 *
 * 用途：将 JSON Schema 转换为 Gemini API 兼容格式
 *
 * 背景：Gemini API 对 JSON Schema 有严格限制，不支持许多标准 JSON Schema 关键字。
 * CLIProxyAPI (Go) 实现了 CleanJSONSchemaForAntigravity 函数进行清理，
 * 本模块是其 Node.js 等效实现。
 *
 * 参考：CLIProxyAPI internal/util/gemini_schema.go
 */

// ============================================================================
// 常量定义
// ============================================================================

/**
 * 验证约束字段 - Gemini 不支持这些约束，需要移到 description 中作为提示
 *
 * 原因：Gemini API 只支持基础的 type、properties、required 等字段，
 * 不支持 JSON Schema 的验证约束关键字。
 * 策略：将这些约束移到 description 中，让模型通过自然语言理解约束。
 */
const VALIDATION_FIELDS = new Set([
  'minLength',      // 字符串最小长度
  'maxLength',      // 字符串最大长度
  'minimum',        // 数值最小值
  'maximum',        // 数值最大值
  'exclusiveMinimum', // 排他性最小值
  'exclusiveMaximum', // 排他性最大值
  'minItems',       // 数组最小元素数
  'maxItems',       // 数组最大元素数
  'minProperties',  // 对象最小属性数
  'maxProperties',  // 对象最大属性数
  'pattern',        // 正则模式
  'format',         // 格式（如 date-time, email 等）
  'multipleOf',     // 数值倍数约束
  'default',        // 默认值 - Claude VALIDATED 模式拒绝
  'examples'        // 示例值 - Claude VALIDATED 模式拒绝
]);

/**
 * 需要完全移除的字段 - Gemini 不支持且无法作为提示保留
 *
 * 原因：这些字段要么是元数据（$schema, $id），要么是 Gemini 不理解的扩展
 */
const FIELDS_TO_REMOVE = new Set([
  '$schema',              // JSON Schema 版本声明
  '$id',                  // Schema 标识符
  '$comment',             // 注释
  'additionalProperties', // 额外属性控制 - 会移到 description 作为提示
  'additionalItems',      // 额外数组项控制
  'uniqueItems',          // 数组元素唯一性
  'propertyNames',        // 属性名约束
  'contentEncoding',      // 内容编码
  'contentMediaType',     // 内容媒体类型
  'if', 'then', 'else',   // 条件 Schema
  'not',                  // 否定 Schema
  'definitions',          // 旧版定义
  '$defs'                 // 新版定义
]);

// ============================================================================
// LRU 缓存
// ============================================================================

/**
 * Schema 清理结果缓存
 *
 * 原因：Schema 清理是 CPU 密集型操作，相同的工具定义会被重复清理。
 * 使用 LRU 缓存可以显著提升性能。
 */
class SchemaLRUCache {
  constructor(maxSize = 50) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  _generateKey(schema) {
    try {
      return JSON.stringify(schema);
    } catch {
      return null;
    }
  }

  get(schema) {
    const key = this._generateKey(schema);
    if (!key) return undefined;
    const value = this.cache.get(key);
    if (value !== undefined) {
      // LRU: 访问时移到末尾
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }

  set(schema, cleaned) {
    const key = this._generateKey(schema);
    if (!key) return;
    if (this.cache.has(key)) {
      this.cache.delete(key);
    } else if (this.cache.size >= this.maxSize) {
      // 移除最旧的条目
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
    }
    this.cache.set(key, cleaned);
  }
}

const schemaCache = new SchemaLRUCache(50);

// ============================================================================
// 核心清理函数
// ============================================================================

/**
 * 清理 JSON Schema 以适配 Gemini API
 *
 * 处理流程（参考 CLIProxyAPI CleanJSONSchemaForAntigravity）：
 *
 * Phase 1: 转换和添加提示
 *   - convertConstToEnum: const → enum（Gemini 不支持 const）
 *   - addEnumHints: enum 值添加到 description
 *   - addAdditionalPropertiesHints: additionalProperties:false → description 提示
 *   - moveConstraintsToDescription: 验证约束移到 description
 *
 * Phase 2: 扁平化复杂结构
 *   - mergeAllOf: 合并 allOf 数组（Gemini 不支持 allOf）
 *   - flattenAnyOfOneOf: 扁平化 anyOf/oneOf（选择第一个或合并）
 *   - flattenTypeArrays: type 数组扁平化为单一类型
 *
 * Phase 3: 清理
 *   - removeUnsupportedKeywords: 移除不支持的关键字
 *   - cleanupRequiredFields: 清理空的 required 数组
 *
 * Phase 4: 空 Schema 占位符
 *   - addEmptySchemaPlaceholder: 为空对象 Schema 添加占位属性
 *     （Claude VALIDATED 模式要求 parameters 不能是空对象）
 *
 * @param {object} schema - 原始 JSON Schema
 * @returns {object} 清理后的 Schema
 */
export function cleanJsonSchema(schema) {
  if (!schema || typeof schema !== 'object') {
    return schema;
  }

  // 检查缓存
  const cached = schemaCache.get(schema);
  if (cached !== undefined) {
    return cached;
  }

  // 深拷贝以避免修改原始对象
  let cleaned;
  try {
    cleaned = JSON.parse(JSON.stringify(schema));
  } catch {
    return schema;
  }

  cleaned = normalizeSchemaShape(cleaned);

  // Phase 1: 转换和添加提示
  cleaned = convertConstToEnum(cleaned);
  cleaned = processSchema(cleaned);

  // Phase 2: 扁平化复杂结构
  cleaned = mergeAllOf(cleaned);
  cleaned = flattenAnyOfOneOf(cleaned);
  cleaned = flattenTypeArrays(cleaned);

  // Phase 3: 清理不支持的关键字
  cleaned = removeUnsupportedKeywords(cleaned);

  // Phase 4: 空 Schema 占位符
  cleaned = addEmptySchemaPlaceholder(cleaned);

  // 缓存结果
  schemaCache.set(schema, cleaned);
  return cleaned;
}

// ============================================================================
// Phase 1: 转换和添加提示
// ============================================================================

/**
 * 将 const 转换为 enum
 *
 * 原因：Gemini API 不支持 JSON Schema 的 const 关键字。
 * 策略：将 { "const": "value" } 转换为 { "enum": ["value"] }
 *
 * 参考：CLIProxyAPI convertConstToEnum
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 转换后的对象
 */
function convertConstToEnum(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(item => convertConstToEnum(item));
  }

  const result = {};
  for (const [key, value] of Object.entries(obj)) {
    if (key === 'const') {
      // const → enum（单值数组）
      result.enum = [value];
    } else if (typeof value === 'object') {
      result[key] = convertConstToEnum(value);
    } else {
      result[key] = value;
    }
  }
  return result;
}

/**
 * 处理 Schema：收集验证约束并移到 description
 *
 * 原因：Gemini 不支持验证约束关键字，但模型可以理解自然语言描述的约束。
 * 策略：将约束信息追加到 description 字段中。
 *
 * 重要：验证约束（如 pattern, format）只在 Schema 定义层级有效，
 * 需要与 properties 中的同名属性区分开。判断方法：
 * - 如果当前对象有 type 字段，说明是 Schema 定义，pattern/format 是约束
 * - 如果当前对象没有 type 字段（如在 properties 容器中），pattern/format 是属性名
 *
 * @param {object} obj - Schema 对象
 * @param {string} path - 当前路径（用于调试）
 * @returns {object} 处理后的对象
 */
function processSchema(obj, path = '') {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map((item, i) => processSchema(item, `${path}[${i}]`));
  }

  const normalizedPath = normalizePath(path);
  const normalized = wrapImplicitProperties(obj, normalizedPath);
  if (normalized !== obj) {
    return processSchema(normalized, path);
  }

  const hints = [];
  const result = {};

  // 判断当前对象是否是 Schema 定义（有 type 字段）
  // 只有在 Schema 定义中，VALIDATION_FIELDS 才是约束关键字
  // 在 properties 容器对象中，这些可能是属性名
  const isPropertiesContainer = isPropertiesContainerPath(normalizedPath);
  const isSchemaDefinition = !isPropertiesContainer && hasSchemaMarkers(obj);

  for (const [key, value] of Object.entries(obj)) {
    // 只有在 Schema 定义中才收集验证约束作为提示
    // 避免将 properties 中名为 pattern/format 的属性误判为约束
    if (isSchemaDefinition && VALIDATION_FIELDS.has(key)) {
      if (value !== null && typeof value === 'object') {
        // 约束字段出现为对象时，保留原结构避免误判
      } else {
        hints.push(`${key}: ${JSON.stringify(value)}`);
        continue; // 不保留原字段
      }
    }

    // additionalProperties: false → 添加提示
    if (key === 'additionalProperties' && isSchemaDefinition) {
      if (value === false) {
        hints.push('No extra properties allowed');
      }
      continue; // 移除此字段
    }

    // enum 值添加到提示
    if (key === 'enum' && Array.isArray(value)) {
      const enumStr = value.map(v => JSON.stringify(v)).join(', ');
      hints.push(`Allowed: ${enumStr}`);
      result[key] = value; // 保留 enum 字段
      continue;
    }

    // 需要完全移除的字段
    if (!isPropertiesContainer && FIELDS_TO_REMOVE.has(key)) {
      continue;
    }

    // 递归处理嵌套对象
    if (typeof value === 'object') {
      result[key] = processSchema(value, `${path}.${key}`);
    } else {
      result[key] = value;
    }
  }

  // 将收集的提示追加到 description
  if (hints.length > 0) {
    const existingDesc = result.description || '';
    const hintsStr = hints.join(', ');
    result.description = existingDesc
      ? `${existingDesc} (${hintsStr})`
      : hintsStr;
  }

  // 清理空的 required 数组
  if (result.required && Array.isArray(result.required) && result.required.length === 0) {
    delete result.required;
  }

  return result;
}

function normalizeSchemaShape(schema) {
  if (!schema || typeof schema !== 'object') return schema;

  if (Array.isArray(schema)) {
    return schema.map(item => normalizeSchemaShape(item));
  }

  const result = { ...schema };

  const normalizedProperties = normalizePropertiesContainer(result.properties);
  if (normalizedProperties) {
    result.properties = normalizedProperties;
  }

  if (Array.isArray(result.required)) {
    const requiredNames = [];
    for (const entry of result.required) {
      if (typeof entry === 'string') {
        requiredNames.push(entry);
        continue;
      }
      if (entry && typeof entry === 'object') {
        const name = entry.key ?? entry.name ?? entry.property ?? entry.prop ?? entry.id ?? null;
        if (name) {
          requiredNames.push(name);
        }
      }
    }
    if (requiredNames.length > 0) {
      result.required = Array.from(new Set(requiredNames));
    }
  }

  for (const [key, value] of Object.entries(result)) {
    if (key === 'properties') continue;
    if (value && typeof value === 'object') {
      result[key] = normalizeSchemaShape(value);
    }
  }

  return result;
}

function normalizePropertiesContainer(properties) {
  if (!properties || typeof properties !== 'object') return null;

  if (Array.isArray(properties)) {
    const normalizedProps = {};
    properties.forEach((item) => {
      if (!item || typeof item !== 'object') return;
      const propName = item.key ?? item.name ?? item.property ?? item.prop ?? item.id ?? null;
      const propSchema = item.value ?? item.schema ?? item.definition ?? item.params ?? item.schemaDef ?? null;

      if (!propName || !propSchema || typeof propSchema !== 'object') {
        return;
      }

      normalizedProps[propName] = unwrapPropertySchema(
        propName,
        normalizeSchemaShape(propSchema)
      );
    });

    return Object.keys(normalizedProps).length > 0 ? normalizedProps : null;
  }

  if (typeof properties === 'object') {
    const entries = Object.entries(properties);
    const numericEntries = entries.length > 0 && entries.every(([key]) => String(Number(key)) === key);
    if (numericEntries) {
      const normalizedProps = {};
      for (const [, item] of entries) {
        if (!item || typeof item !== 'object') continue;
        const propName = item.key ?? item.name ?? item.property ?? item.prop ?? item.id ?? null;
        const propSchema = item.value ?? item.schema ?? item.definition ?? item.params ?? item.schemaDef ?? null;
        if (!propName || !propSchema || typeof propSchema !== 'object') {
          continue;
        }
        normalizedProps[propName] = unwrapPropertySchema(
          propName,
          normalizeSchemaShape(propSchema)
        );
      }
      if (Object.keys(normalizedProps).length > 0) {
        return normalizedProps;
      }
    }

    const processed = {};
    for (const [key, value] of entries) {
      processed[key] = unwrapPropertySchema(
        key,
        normalizeSchemaShape(value)
      );
    }
    return processed;
  }

  return null;
}

function unwrapPropertySchema(propName, schema) {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) {
    return schema;
  }

  if ((schema.key === propName || schema.name === propName) && schema.value) {
    return normalizeSchemaShape(schema.value);
  }

  const keys = Object.keys(schema);
  if (keys.length === 1 && keys[0] === propName) {
    const inner = schema[propName];
    if (inner && typeof inner === 'object' && looksLikeSchemaNode(inner)) {
      return normalizeSchemaShape(inner);
    }
  }

  return schema;
}

function wrapImplicitProperties(obj, path) {
  if (isPropertiesContainerPath(path)) return obj;
  if (!shouldWrapImplicitProperties(obj)) return obj;
  return {
    type: 'object',
    properties: obj
  };
}

function isPropertiesContainerPath(path) {
  if (!path) return false;
  return path === 'properties' || path.endsWith('.properties');
}

function normalizePath(path) {
  if (!path) return '';
  return path.startsWith('.') ? path.slice(1) : path;
}

function shouldWrapImplicitProperties(obj) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return false;

  const keys = Object.keys(obj);
  if (keys.length === 0) return false;
  if (hasSchemaMarkers(obj)) return false;

  let hasSchemaLikeChild = false;
  for (const value of Object.values(obj)) {
    if (!looksLikeSchemaNode(value)) {
      return false;
    }
    hasSchemaLikeChild = true;
  }

  return hasSchemaLikeChild;
}

function hasSchemaMarkers(obj) {
  const markers = new Set([
    'type',
    'properties',
    'required',
    'items',
    'anyOf',
    'oneOf',
    'allOf',
    'enum',
    'const',
    'description',
    'title',
    '$ref',
    'additionalProperties'
  ]);

  for (const [key, value] of Object.entries(obj)) {
    if (markers.has(key)) return true;
    if (VALIDATION_FIELDS.has(key) && !(key === 'pattern' && typeof value === 'object')) {
      return true;
    }
  }

  return false;
}

function looksLikeSchemaNode(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;

  return (
    'type' in value ||
    'properties' in value ||
    'items' in value ||
    'enum' in value ||
    'anyOf' in value ||
    'oneOf' in value ||
    'allOf' in value ||
    '$ref' in value ||
    'description' in value ||
    'title' in value
  );
}

// ============================================================================
// Phase 2: 扁平化复杂结构
// ============================================================================

/**
 * 合并 allOf 数组
 *
 * 原因：Gemini API 不支持 allOf 关键字。
 * 策略：将 allOf 中的所有 Schema 合并为一个扁平对象。
 *
 * 示例：
 *   { "allOf": [{ "type": "object", "properties": { "a": {...} } }, { "properties": { "b": {...} } }] }
 *   →
 *   { "type": "object", "properties": { "a": {...}, "b": {...} } }
 *
 * 参考：CLIProxyAPI mergeAllOf
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 合并后的对象
 */
function mergeAllOf(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(item => mergeAllOf(item));
  }

  // 先递归处理子对象
  let result = {};
  for (const [key, value] of Object.entries(obj)) {
    if (key === 'allOf' && Array.isArray(value)) {
      // 合并 allOf 中的所有 Schema
      for (const subSchema of value) {
        const processed = mergeAllOf(subSchema);
        result = deepMergeSchemas(result, processed);
      }
    } else if (typeof value === 'object') {
      result[key] = mergeAllOf(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

/**
 * 深度合并两个 Schema 对象
 *
 * 策略：
 * - properties: 合并所有属性
 * - required: 合并所有必需字段
 * - 其他字段: 后者覆盖前者
 *
 * @param {object} base - 基础 Schema
 * @param {object} override - 覆盖 Schema
 * @returns {object} 合并后的 Schema
 */
function deepMergeSchemas(base, override) {
  if (!base || typeof base !== 'object') return override;
  if (!override || typeof override !== 'object') return base;

  const result = { ...base };

  for (const [key, value] of Object.entries(override)) {
    if (key === 'properties' && result.properties) {
      // 合并 properties
      result.properties = { ...result.properties, ...value };
    } else if (key === 'required' && Array.isArray(result.required) && Array.isArray(value)) {
      // 合并 required（去重）
      result.required = [...new Set([...result.required, ...value])];
    } else if (typeof value === 'object' && typeof result[key] === 'object' && !Array.isArray(value)) {
      // 递归合并嵌套对象
      result[key] = deepMergeSchemas(result[key], value);
    } else {
      // 直接覆盖
      result[key] = value;
    }
  }

  return result;
}

/**
 * 扁平化 anyOf/oneOf
 *
 * 原因：Gemini API 对 anyOf/oneOf 的支持有限，复杂的联合类型容易导致错误。
 * 策略：
 * - 如果只有 2 个选项且其中一个是 null，转换为 nullable 类型
 * - 否则选择第一个非 null 类型，并在 description 中说明其他可能的类型
 *
 * 参考：CLIProxyAPI flattenAnyOfOneOf
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 扁平化后的对象
 */
function flattenAnyOfOneOf(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(item => flattenAnyOfOneOf(item));
  }

  const result = {};

  for (const [key, value] of Object.entries(obj)) {
    if ((key === 'anyOf' || key === 'oneOf') && Array.isArray(value)) {
      // 分离 null 和非 null 类型
      const nullTypes = value.filter(v => v?.type === 'null');
      const nonNullTypes = value.filter(v => v?.type !== 'null');

      if (nonNullTypes.length === 0) {
        // 只有 null 类型
        result.type = 'null';
      } else if (nonNullTypes.length === 1) {
        // 只有一个非 null 类型（可能是 nullable）
        const mainType = flattenAnyOfOneOf(nonNullTypes[0]);
        Object.assign(result, mainType);
        if (nullTypes.length > 0) {
          // 添加 nullable 提示
          result.description = result.description
            ? `${result.description} (nullable)`
            : 'nullable';
        }
      } else {
        // 多个非 null 类型：选择第一个，描述其他可能
        const mainType = flattenAnyOfOneOf(nonNullTypes[0]);
        Object.assign(result, mainType);

        // 收集其他类型作为提示
        const otherTypes = nonNullTypes.slice(1).map(t => t.type || 'object').join(' | ');
        const typeHint = `Also accepts: ${otherTypes}`;
        result.description = result.description
          ? `${result.description}. ${typeHint}`
          : typeHint;
      }
    } else if (typeof value === 'object') {
      result[key] = flattenAnyOfOneOf(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

/**
 * 扁平化 type 数组
 *
 * 原因：Gemini API 不支持 type 为数组的形式（如 ["string", "null"]）。
 * 策略：选择第一个非 null 类型，并在 description 中标注 nullable。
 *
 * 示例：
 *   { "type": ["string", "null"] }
 *   →
 *   { "type": "string", "description": "nullable" }
 *
 * 参考：CLIProxyAPI flattenTypeArrays
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 扁平化后的对象
 */
function flattenTypeArrays(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(item => flattenTypeArrays(item));
  }

  const result = {};

  for (const [key, value] of Object.entries(obj)) {
    if (key === 'type' && Array.isArray(value)) {
      // type 数组扁平化
      const nonNullTypes = value.filter(t => t !== 'null');
      const hasNull = value.includes('null');

      if (nonNullTypes.length > 0) {
        result.type = nonNullTypes[0]; // 选择第一个非 null 类型
        if (hasNull) {
          result.description = result.description
            ? `${result.description} (nullable)`
            : 'nullable';
        }
      } else {
        result.type = 'null';
      }
    } else if (typeof value === 'object') {
      result[key] = flattenTypeArrays(value);
    } else {
      result[key] = value;
    }
  }

  return result;
}

// ============================================================================
// Phase 3: 清理不支持的关键字
// ============================================================================

/**
 * 移除不支持的关键字
 *
 * 原因：Gemini API 会拒绝包含未知关键字的 Schema。
 * 策略：递归移除 FIELDS_TO_REMOVE 中的所有字段。
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 清理后的对象
 */
function removeUnsupportedKeywords(obj, path = '') {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map((item, i) => removeUnsupportedKeywords(item, `${path}[${i}]`));
  }

  const normalizedPath = normalizePath(path);
  const isPropertiesContainer = isPropertiesContainerPath(normalizedPath);
  const result = {};

  for (const [key, value] of Object.entries(obj)) {
    if (!isPropertiesContainer && FIELDS_TO_REMOVE.has(key)) {
      continue; // 跳过不支持的字段
    }
    if (typeof value === 'object') {
      result[key] = removeUnsupportedKeywords(value, `${path}.${key}`);
    } else {
      result[key] = value;
    }
  }

  return result;
}

// ============================================================================
// Phase 4: 空 Schema 占位符
// ============================================================================

/**
 * 为空对象 Schema 添加占位属性
 *
 * 原因：Claude VALIDATED 模式要求工具的 parameters 不能是空对象。
 * 如果 Schema 是 { "type": "object" } 且没有 properties，会导致验证失败。
 *
 * 策略：添加一个可选的 reason 属性作为占位符。
 *
 * 参考：CLIProxyAPI addEmptySchemaPlaceholder
 *
 * @param {object} obj - Schema 对象
 * @returns {object} 添加占位符后的对象
 */
function addEmptySchemaPlaceholder(obj) {
  if (!obj || typeof obj !== 'object') return obj;

  if (Array.isArray(obj)) {
    return obj.map(item => addEmptySchemaPlaceholder(item));
  }

  const result = { ...obj };

  // 检查是否是空对象 Schema
  if (result.type === 'object') {
    const hasProperties = result.properties && Object.keys(result.properties).length > 0;

    if (!hasProperties) {
      // 添加占位属性
      result.properties = {
        reason: {
          type: 'string',
          description: 'Optional reason or context for this action'
        }
      };
      // 注意：不添加到 required，保持为可选属性
    }
  }

  // 递归处理嵌套对象
  for (const [key, value] of Object.entries(result)) {
    if (typeof value === 'object' && key !== 'properties') {
      result[key] = addEmptySchemaPlaceholder(value);
    } else if (key === 'properties' && typeof value === 'object') {
      // 对 properties 中的每个属性也进行处理
      const processedProps = {};
      for (const [propKey, propValue] of Object.entries(value)) {
        processedProps[propKey] = addEmptySchemaPlaceholder(propValue);
      }
      result.properties = processedProps;
    }
  }

  return result;
}

// ============================================================================
// 消息合并工具
// ============================================================================

/**
 * 合并连续相同角色的消息
 *
 * 原因：Gemini API 要求 user/model 消息严格交替出现。
 * 如果有连续的 user 或 model 消息，需要合并它们的 parts。
 *
 * @param {Array} contents - Gemini contents 数组
 * @returns {Array} 合并后的 contents 数组
 */
export function mergeConsecutiveRoles(contents) {
  if (!contents || contents.length === 0) return [];

  const merged = [];
  for (const content of contents) {
    const last = merged[merged.length - 1];
    if (last && last.role === content.role) {
      // 合并 parts
      last.parts.push(...content.parts);
    } else {
      // 新角色，创建新消息
      merged.push({ role: content.role, parts: [...content.parts] });
    }
  }
  return merged;
}
