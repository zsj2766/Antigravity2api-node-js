/**
 * 转换器单元测试 - 内容类型转换
 *
 * 覆盖类型：
 * - Text: 字符串/数组形式互转
 * - Image: base64/URL 格式互转
 * - Document: base64/file_id 格式互转
 * - Tool Use: Claude tool_use <-> OpenAI tool_calls
 * - Tool Result: 含 is_error 场景
 * - 边界情况: null、空数组、不完整格式
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';

// Claude -> OpenAI
import {
  convertClaudeContentToOpenAI,
  extractToolUsesAsOpenAIToolCalls
} from '../../src/utils/converters/claudeToOpenaiAdapter.js';

// OpenAI -> Claude
import {
  convertOpenAIContentToClaude,
  convertOpenAIImageToClaude,
  convertOpenAIFileToClaude,
  convertOpenAIToolCallsToClaude,
  convertOpenAIToolResultToClaude
} from '../../src/utils/converters/openaiToClaudeAdapter.js';

// Claude -> Gemini
import {
  convertClaudeImageToGemini,
  convertClaudeDocumentToGemini,
  extractMediaFromToolResult
} from '../../src/utils/converters/common/imageUtils.js';

// Mock Base64 Data (1x1 pixel PNG)
const MOCK_IMAGE_BASE64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=';
const MOCK_PDF_BASE64 = 'JVBERi0xLjQKJeLjz9MKMSAwIG9iago8PAovVHlwZSAvQ2F0YWxvZwo+PgplbmRvYmoKdHJhaWxlcgo8PAovUm9vdCAxIDAgUgo+Pg==';

// ==================== 1. Text Conversion Tests ====================

describe('Text Conversion', () => {
  test('Claude string to OpenAI - direct string', () => {
    const result = convertClaudeContentToOpenAI('Hello world');
    assert.strictEqual(result.content, 'Hello world');
    assert.deepStrictEqual(result.toolCalls, []);
  });

  test('Claude text block array to OpenAI', () => {
    const input = [{ type: 'text', text: 'Hello' }];
    const result = convertClaudeContentToOpenAI(input);
    assert.strictEqual(result.content, 'Hello');
  });

  test('Claude multiple text blocks to OpenAI', () => {
    const input = [
      { type: 'text', text: 'Hello' },
      { type: 'text', text: 'World' }
    ];
    const result = convertClaudeContentToOpenAI(input);
    assert.strictEqual(result.content, 'Hello\nWorld');
  });

  test('OpenAI string to Claude', () => {
    const result = convertOpenAIContentToClaude('Hello');
    assert.deepStrictEqual(result, [{ type: 'text', text: 'Hello' }]);
  });

  test('OpenAI text block array to Claude', () => {
    const input = [{ type: 'text', text: 'Hello' }];
    const result = convertOpenAIContentToClaude(input);
    assert.deepStrictEqual(result, [{ type: 'text', text: 'Hello' }]);
  });

  test('OpenAI input_text (Responses API) to Claude', () => {
    const input = [{ type: 'input_text', text: 'Hello from Responses API' }];
    const result = convertOpenAIContentToClaude(input);
    assert.deepStrictEqual(result, [{ type: 'text', text: 'Hello from Responses API' }]);
  });
});

// ==================== 2. Image Conversion Tests ====================

describe('Image Conversion', () => {
  // Claude -> Gemini
  test('Claude base64 image to Gemini inlineData', () => {
    const claudeImage = {
      type: 'image',
      source: {
        type: 'base64',
        media_type: 'image/png',
        data: MOCK_IMAGE_BASE64
      }
    };
    const result = convertClaudeImageToGemini(claudeImage);
    assert.deepStrictEqual(result, {
      inlineData: {
        mimeType: 'image/png',
        data: MOCK_IMAGE_BASE64
      }
    });
  });

  test('Claude URL image to Gemini fileData', () => {
    const claudeImage = {
      type: 'image',
      source: {
        type: 'url',
        url: 'https://example.com/image.jpg',
        media_type: 'image/jpeg'
      }
    };
    const result = convertClaudeImageToGemini(claudeImage);
    assert.deepStrictEqual(result, {
      fileData: {
        fileUri: 'https://example.com/image.jpg',
        mimeType: 'image/jpeg'
      }
    });
  });

  // OpenAI -> Claude
  test('OpenAI base64 image_url to Claude', () => {
    const openAIImage = {
      url: `data:image/png;base64,${MOCK_IMAGE_BASE64}`
    };
    const result = convertOpenAIImageToClaude(openAIImage);
    assert.deepStrictEqual(result, {
      type: 'image',
      source: {
        type: 'base64',
        media_type: 'image/png',
        data: MOCK_IMAGE_BASE64
      }
    });
  });

  test('OpenAI URL image_url to Claude', () => {
    const openAIImage = {
      url: 'https://example.com/image.jpg'
    };
    const result = convertOpenAIImageToClaude(openAIImage);
    assert.deepStrictEqual(result, {
      type: 'image',
      source: {
        type: 'url',
        url: 'https://example.com/image.jpg'
      }
    });
  });

  test('OpenAI input_image (Responses API) to Claude', () => {
    const input = [{ type: 'input_image', image_url: 'https://example.com/image.jpg' }];
    const result = convertOpenAIContentToClaude(input);
    assert.strictEqual(result[0].type, 'image');
    assert.strictEqual(result[0].source.type, 'url');
  });

  // Claude -> OpenAI
  test('Claude base64 image in content to OpenAI', () => {
    const content = [{
      type: 'image',
      source: {
        type: 'base64',
        media_type: 'image/png',
        data: MOCK_IMAGE_BASE64
      }
    }];
    const result = convertClaudeContentToOpenAI(content);
    assert.ok(Array.isArray(result.content));
    assert.strictEqual(result.content[0].type, 'image_url');
    assert.ok(result.content[0].image_url.url.includes('base64'));
  });
});

// ==================== 3. Document Conversion Tests ====================

describe('Document Conversion', () => {
  // Claude -> Gemini
  test('Claude base64 document to Gemini inlineData', () => {
    const claudeDoc = {
      type: 'document',
      source: {
        type: 'base64',
        media_type: 'application/pdf',
        data: MOCK_PDF_BASE64
      }
    };
    const result = convertClaudeDocumentToGemini(claudeDoc);
    assert.deepStrictEqual(result, {
      inlineData: {
        mimeType: 'application/pdf',
        data: MOCK_PDF_BASE64
      }
    });
  });

  test('Claude URL document to Gemini fileData', () => {
    const claudeDoc = {
      type: 'document',
      source: {
        type: 'url',
        url: 'https://example.com/doc.pdf',
        media_type: 'application/pdf'
      }
    };
    const result = convertClaudeDocumentToGemini(claudeDoc);
    assert.deepStrictEqual(result, {
      fileData: {
        fileUri: 'https://example.com/doc.pdf',
        mimeType: 'application/pdf'
      }
    });
  });

  // OpenAI -> Claude
  test('OpenAI input_file with file_data to Claude document', () => {
    const filePart = {
      type: 'input_file',
      filename: 'document.pdf',
      file_data: `data:application/pdf;base64,${MOCK_PDF_BASE64}`
    };
    const result = convertOpenAIFileToClaude(filePart);
    assert.strictEqual(result.type, 'document');
    assert.strictEqual(result.source.type, 'base64');
    assert.strictEqual(result.source.media_type, 'application/pdf');
    assert.strictEqual(result.title, 'document.pdf');
  });

  test('OpenAI input_file with file_id to Claude document', () => {
    const filePart = {
      type: 'input_file',
      file_id: 'file-abc123'
    };
    const result = convertOpenAIFileToClaude(filePart);
    assert.strictEqual(result.type, 'document');
    assert.strictEqual(result.source.type, 'file');
    assert.strictEqual(result.source.file_id, 'file-abc123');
  });

  // Claude document in content -> OpenAI (placeholder)
  test('Claude document in content to OpenAI placeholder', () => {
    const content = [{
      type: 'document',
      source: {
        type: 'base64',
        media_type: 'application/pdf',
        data: MOCK_PDF_BASE64
      }
    }];
    const result = convertClaudeContentToOpenAI(content);
    assert.ok(result.content.includes('[Document: application/pdf]'));
  });
});

// ==================== 4. Tool Use Conversion Tests ====================

describe('Tool Use Conversion', () => {
  // Claude -> OpenAI
  test('Claude tool_use to OpenAI tool_calls', () => {
    const claudeToolUse = [{
      type: 'tool_use',
      id: 'toolu_01D7FLrfh4GYq7yT1ULFeyMV',
      name: 'get_weather',
      input: { location: 'San Francisco' }
    }];
    const result = extractToolUsesAsOpenAIToolCalls(claudeToolUse);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].id, 'toolu_01D7FLrfh4GYq7yT1ULFeyMV');
    assert.strictEqual(result[0].type, 'function');
    assert.strictEqual(result[0].function.name, 'get_weather');
    assert.strictEqual(result[0].function.arguments, '{"location":"San Francisco"}');
  });

  test('Claude multiple tool_use to OpenAI', () => {
    const claudeToolUses = [
      { type: 'tool_use', id: 'call_1', name: 'fn1', input: { a: 1 } },
      { type: 'tool_use', id: 'call_2', name: 'fn2', input: { b: 2 } }
    ];
    const result = extractToolUsesAsOpenAIToolCalls(claudeToolUses);
    assert.strictEqual(result.length, 2);
    assert.strictEqual(result[0].function.name, 'fn1');
    assert.strictEqual(result[1].function.name, 'fn2');
  });

  // OpenAI -> Claude
  test('OpenAI tool_calls to Claude tool_use', () => {
    const openAIToolCalls = [{
      id: 'call_abc123',
      type: 'function',
      function: {
        name: 'get_weather',
        arguments: '{"location":"San Francisco"}'
      }
    }];
    const result = convertOpenAIToolCallsToClaude(openAIToolCalls);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].type, 'tool_use');
    assert.strictEqual(result[0].id, 'call_abc123');
    assert.strictEqual(result[0].name, 'get_weather');
    assert.deepStrictEqual(result[0].input, { location: 'San Francisco' });
  });

  // Claude tool_use in content
  test('Claude content with tool_use extracts toolCalls', () => {
    const content = [
      { type: 'text', text: 'Let me check the weather.' },
      { type: 'tool_use', id: 'call_1', name: 'get_weather', input: { location: 'NYC' } }
    ];
    const result = convertClaudeContentToOpenAI(content);
    assert.strictEqual(result.toolCalls.length, 1);
    assert.strictEqual(result.toolCalls[0].function.name, 'get_weather');
  });
});

// ==================== 5. Tool Result Conversion Tests ====================

describe('Tool Result Conversion', () => {
  // OpenAI -> Claude
  test('OpenAI tool message to Claude tool_result', () => {
    const openAIMsg = {
      role: 'tool',
      tool_call_id: 'call_abc123',
      content: '{"temperature": 72, "condition": "sunny"}'
    };
    const result = convertOpenAIToolResultToClaude(openAIMsg);
    assert.strictEqual(result.type, 'tool_result');
    assert.strictEqual(result.tool_use_id, 'call_abc123');
    assert.ok(Array.isArray(result.content));
  });

  test('OpenAI tool error message to Claude tool_result with is_error', () => {
    const openAIMsg = {
      role: 'tool',
      tool_call_id: 'call_abc123',
      content: 'Error: Location not found'
    };
    const result = convertOpenAIToolResultToClaude(openAIMsg);
    assert.strictEqual(result.type, 'tool_result');
    assert.strictEqual(result.is_error, true);
  });

  test('OpenAI tool success message should not have is_error', () => {
    const openAIMsg = {
      role: 'tool',
      tool_call_id: 'call_abc123',
      content: 'Success: Data retrieved'
    };
    const result = convertOpenAIToolResultToClaude(openAIMsg);
    assert.strictEqual(result.is_error, undefined);
  });

  // Claude tool_result with nested image
  test('extractMediaFromToolResult with nested image', () => {
    const content = [
      { type: 'text', text: 'Screenshot:' },
      {
        type: 'image',
        source: {
          type: 'base64',
          media_type: 'image/png',
          data: MOCK_IMAGE_BASE64
        }
      }
    ];
    const result = extractMediaFromToolResult(content);
    assert.strictEqual(result.text, 'Screenshot:');
    assert.strictEqual(result.images.length, 1);
    assert.strictEqual(result.images[0].inlineData.mimeType, 'image/png');
  });

  test('extractMediaFromToolResult with string content', () => {
    const result = extractMediaFromToolResult('Simple text result');
    assert.strictEqual(result.text, 'Simple text result');
    assert.deepStrictEqual(result.images, []);
    assert.deepStrictEqual(result.documents, []);
  });

  test('extractMediaFromToolResult with nested document', () => {
    const content = [
      { type: 'text', text: 'PDF content:' },
      {
        type: 'document',
        source: {
          type: 'base64',
          media_type: 'application/pdf',
          data: MOCK_PDF_BASE64
        }
      }
    ];
    const result = extractMediaFromToolResult(content);
    assert.strictEqual(result.documents.length, 1);
    assert.strictEqual(result.documents[0].inlineData.mimeType, 'application/pdf');
  });
});

// ==================== 6. Edge Cases ====================

describe('Edge Cases', () => {
  // Empty/null inputs
  test('convertClaudeContentToOpenAI with empty string', () => {
    const result = convertClaudeContentToOpenAI('');
    assert.strictEqual(result.content, '');
  });

  test('convertClaudeContentToOpenAI with empty array', () => {
    const result = convertClaudeContentToOpenAI([]);
    assert.strictEqual(result.content, '');
  });

  test('convertClaudeContentToOpenAI with null', () => {
    const result = convertClaudeContentToOpenAI(null);
    assert.strictEqual(result.content, '');
  });

  test('convertClaudeContentToOpenAI with undefined', () => {
    const result = convertClaudeContentToOpenAI(undefined);
    assert.strictEqual(result.content, '');
  });

  test('convertOpenAIContentToClaude with empty string', () => {
    const result = convertOpenAIContentToClaude('');
    assert.deepStrictEqual(result, [{ type: 'text', text: '' }]);
  });

  test('convertOpenAIContentToClaude with empty array', () => {
    const result = convertOpenAIContentToClaude([]);
    assert.deepStrictEqual(result, [{ type: 'text', text: '' }]);
  });

  test('extractToolUsesAsOpenAIToolCalls with non-array', () => {
    const result = extractToolUsesAsOpenAIToolCalls('not an array');
    assert.deepStrictEqual(result, []);
  });

  test('extractToolUsesAsOpenAIToolCalls with null', () => {
    const result = extractToolUsesAsOpenAIToolCalls(null);
    assert.deepStrictEqual(result, []);
  });

  // Incomplete format
  test('Claude image with missing source', () => {
    const result = convertClaudeImageToGemini({ type: 'image' });
    assert.strictEqual(result, null);
  });

  test('Claude image with incomplete source', () => {
    const result = convertClaudeImageToGemini({
      type: 'image',
      source: { type: 'base64' }
    });
    assert.strictEqual(result, null);
  });

  test('OpenAI image_url with missing url', () => {
    const result = convertOpenAIImageToClaude({});
    assert.strictEqual(result, null);
  });

  test('OpenAI image_url with null', () => {
    const result = convertOpenAIImageToClaude(null);
    assert.strictEqual(result, null);
  });

  test('Claude tool_use with missing fields uses defaults', () => {
    const input = [{ type: 'tool_use' }];
    const result = extractToolUsesAsOpenAIToolCalls(input);
    assert.strictEqual(result.length, 1);
    assert.strictEqual(result[0].function.name, 'unknown');
    // After fix: safeJsonStringify(undefined) || '{}' now correctly returns '{}'
    assert.strictEqual(result[0].function.arguments, '{}');
  });

  // Mixed content types
  test('Claude mixed content with text and image', () => {
    const content = [
      { type: 'text', text: 'Check this image:' },
      {
        type: 'image',
        source: {
          type: 'base64',
          media_type: 'image/png',
          data: MOCK_IMAGE_BASE64
        }
      }
    ];
    const result = convertClaudeContentToOpenAI(content);
    assert.ok(Array.isArray(result.content));
    assert.strictEqual(result.content.length, 2);
    assert.strictEqual(result.content[0].type, 'text');
    assert.strictEqual(result.content[1].type, 'image_url');
  });

  // Whitespace handling
  test('Claude text with only whitespace', () => {
    const content = [{ type: 'text', text: '   ' }];
    const result = convertClaudeContentToOpenAI(content);
    // Whitespace-only text should be filtered
    assert.strictEqual(result.content, '');
  });
});
