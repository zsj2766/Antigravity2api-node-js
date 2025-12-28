/**
 * JSON Schema 清理工具
 * 移除 Gemini 不支持的字段
 */

// JSON Schema 清理相关常量
const VALIDATION_FIELDS = {
  'minLength': 'minLength',
  'maxLength': 'maxLength',
  'minimum': 'minimum',
  'maximum': 'maximum',
  'minItems': 'minItems',
  'maxItems': 'maxItems',
  'minProperties': 'minProperties',
  'maxProperties': 'maxProperties',
  'pattern': 'pattern',
  'format': 'format',
  'multipleOf': 'multipleOf'
};

const FIELDS_TO_REMOVE = new Set([
  '$schema',
  'additionalProperties',
  'uniqueItems',
  'exclusiveMinimum',
  'exclusiveMaximum'
]);

/**
 * 简单的 LRU 缓存实现，用于缓存清理后的 JSON Schema
 * 避免对相同的工具定义重复进行深度清理
 */
class SchemaLRUCache {
  constructor(maxSize = 50) {
    this.maxSize = maxSize;
    this.cache = new Map();
  }

  /**
   * 生成缓存键：使用 JSON 序列化作为唯一标识
   */
  _generateKey(schema) {
    try {
      return JSON.stringify(schema);
    } catch {
      return null; // 无法序列化的对象不缓存
    }
  }

  get(schema) {
    const key = this._generateKey(schema);
    if (!key) return undefined;

    const value = this.cache.get(key);
    if (value !== undefined) {
      // LRU: 访问后移到末尾（最近使用）
      this.cache.delete(key);
      this.cache.set(key, value);
    }
    return value;
  }

  set(schema, cleaned) {
    const key = this._generateKey(schema);
    if (!key) return;

    // 如果已存在，先删除以更新位置
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }
    // 超过容量时删除最旧的（Map 迭代顺序为插入顺序）
    else if (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      this.cache.delete(oldestKey);
    }

    this.cache.set(key, cleaned);
  }
}

const schemaCache = new SchemaLRUCache(50);

/**
 * 清理 JSON Schema，移除 Gemini 不支持的字段
 * 使用 LRU 缓存避免重复清理相同的 Schema
 */
function cleanJsonSchema(schema) {
  if (!schema || typeof schema !== 'object') {
    return schema;
  }

  // 尝试从缓存获取
  const cached = schemaCache.get(schema);
  if (cached !== undefined) {
    return cached;
  }

  // 收集验证信息（从所有层级）
  const collectValidations = (obj) => {
    const validations = [];

    for (const [field, value] of Object.entries(VALIDATION_FIELDS)) {
      if (field in obj) {
        validations.push(`${field}: ${value}`);
        delete obj[field];
      }
    }

    for (const field of FIELDS_TO_REMOVE) {
      if (field in obj) {
        if (field === 'additionalProperties' && obj[field] === false) {
          validations.push('no additional properties');
        }
        delete obj[field];
      }
    }

    return validations;
  };

  // 递归清理嵌套对象
  const cleanObject = (obj, path = '') => {
    if (Array.isArray(obj)) {
      return obj.map(item => typeof item === 'object' ? cleanObject(item, path) : item);
    } else if (obj && typeof obj === 'object') {
      // 先收集当前层的验证信息
      const validations = collectValidations(obj);

      const cleaned = {};
      for (const [key, value] of Object.entries(obj)) {
        if (FIELDS_TO_REMOVE.has(key)) continue;
        if (key in VALIDATION_FIELDS) continue;

        if (key === 'description' && validations.length > 0 && path === '') {
          // 只在顶层追加验证要求
          cleaned[key] = `${value || ''} (${validations.join(', ')})`.trim();
        } else {
          cleaned[key] = typeof value === 'object' ? cleanObject(value, `${path}.${key}`) : value;
        }
      }

      // 处理 required 数组
      if (cleaned.required && Array.isArray(cleaned.required)) {
        // 确保 required 不为空数组
        if (cleaned.required.length === 0) {
          delete cleaned.required;
        }
      }

      return cleaned;
    }
    return obj;
  };

  const result = cleanObject(schema);

  // 存入缓存
  schemaCache.set(schema, result);

  return result;
}

export {
  VALIDATION_FIELDS,
  FIELDS_TO_REMOVE,
  SchemaLRUCache,
  schemaCache,
  cleanJsonSchema
};
