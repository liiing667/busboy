'use strict';

const assert = require('assert');

const busboy = require('..');

const boundary = '----busboyTransferEncodingTest';

function makeForm(parts) {
  const chunks = [];
  for (const entry of parts) {
    chunks.push(Buffer.from(`--${boundary}\r\n`, 'latin1'));
    for (const header of entry.headers)
      chunks.push(Buffer.from(`${header}\r\n`, 'latin1'));
    chunks.push(Buffer.from('\r\n', 'latin1'));
    chunks.push(entry.body);
    chunks.push(Buffer.from('\r\n', 'latin1'));
  }
  chunks.push(Buffer.from(`--${boundary}--\r\n`, 'latin1'));
  return Buffer.concat(chunks);
}

function part(name, encoding, body) {
  const headers = [
    `Content-Disposition: form-data; name="${name}"; filename="${name}.bin"`,
    'Content-Type: application/octet-stream',
  ];
  if (encoding !== undefined)
    headers.push(`Content-Transfer-Encoding: ${encoding}`);
  return {
    headers,
    body: (Buffer.isBuffer(body) ? body : Buffer.from(body)),
  };
}

function field(name, encoding, body) {
  const headers = [
    `Content-Disposition: form-data; name="${name}"`,
  ];
  if (encoding !== undefined)
    headers.push(`Content-Transfer-Encoding: ${encoding}`);
  return {
    headers,
    body: (Buffer.isBuffer(body) ? body : Buffer.from(body)),
  };
}

function parse(input, byByte) {
  return new Promise((resolve) => {
    const bb = busboy({
      headers: {
        'content-type': `multipart/form-data; boundary=${boundary}`,
      },
    });
    const results = [];

    bb.on('file', (name, stream, info) => {
      const chunks = [];
      const result = {
        type: 'file',
        name,
        info,
        data: null,
        limited: false,
      };
      results.push(result);
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('limit', () => {
        result.limited = true;
      });
      stream.on('error', (err) => {
        result.err = err.message;
      });
      stream.on('close', () => {
        result.data = Buffer.concat(chunks);
        assert.strictEqual(stream.truncated, result.limited);
      });
    });

    bb.on('field', (name, value, info) => {
      results.push({ type: 'field', name, value, info });
    });

    bb.on('error', (err) => {
      results.push({ type: 'error', message: err.message });
    });

    bb.on('close', () => resolve(results));

    if (byByte) {
      for (let i = 0; i < input.length; ++i)
        bb.write(input.slice(i, i + 1));
    } else {
      bb.write(input);
    }
    bb.end();
  });
}

async function runBoth(input, expected) {
  await Promise.all([false, true].map(async (byByte) => {
    const results = await parse(input, byByte);
    try {
      assert.deepStrictEqual(results, expected);
    } catch (err) {
      err.message += ` (${byByte ? 'byte-by-byte' : 'single write'})`;
      throw err;
    }
  }));
}

async function main() {
  const base64Hello = Buffer.from('hello\x00world');
  const binary = Buffer.from([0x80, 0xff, 0x00, 0x41]);

  await runBoth(
    makeForm([
      part('one', 'base64', base64Hello.toString('base64')),
      part('two', 'x-unknown', 'must not be delivered'),
      part('three', 'base64', Buffer.from('world').toString('base64')),
      part('four', '7bit', 'seven'),
      part('five', '8bit', binary),
      part('bad2', 'x-unknown-2', 'also not delivered'),
      part('six', '', 'empty'),
      part('seven', undefined, 'default'),
    ]),
    [
      { type: 'file',
        name: 'one',
        info: {
          filename: 'one.bin',
          encoding: 'base64',
          mimeType: 'application/octet-stream',
        },
        data: base64Hello,
        limited: false },
      { type: 'error',
        message: 'Unsupported content transfer encoding: x-unknown' },
      { type: 'file',
        name: 'three',
        info: {
          filename: 'three.bin',
          encoding: 'base64',
          mimeType: 'application/octet-stream',
        },
        data: Buffer.from('world'),
        limited: false },
      { type: 'file',
        name: 'four',
        info: {
          filename: 'four.bin',
          encoding: '7bit',
          mimeType: 'application/octet-stream',
        },
        data: Buffer.from('seven'),
        limited: false },
      { type: 'file',
        name: 'five',
        info: {
          filename: 'five.bin',
          encoding: '8bit',
          mimeType: 'application/octet-stream',
        },
        data: binary,
        limited: false },
      { type: 'error',
        message: 'Unsupported content transfer encoding: x-unknown-2' },
      { type: 'file',
        name: 'six',
        info: {
          filename: 'six.bin',
          encoding: '',
          mimeType: 'application/octet-stream',
        },
        data: Buffer.from('empty'),
        limited: false },
      { type: 'file',
        name: 'seven',
        info: {
          filename: 'seven.bin',
          encoding: '7bit',
          mimeType: 'application/octet-stream',
        },
        data: Buffer.from('default'),
        limited: false },
    ]
  );

  await runBoth(
    makeForm([
      field('b64', 'base64', Buffer.from('Hello').toString('base64')),
      field('plain', undefined, 'ok'),
    ]),
    [
      { type: 'field',
        name: 'b64',
        value: 'Hello',
        info: {
          nameTruncated: false,
          valueTruncated: false,
          encoding: 'base64',
          mimeType: 'text/plain',
        } },
      { type: 'field',
        name: 'plain',
        value: 'ok',
        info: {
          nameTruncated: false,
          valueTruncated: false,
          encoding: '7bit',
          mimeType: 'text/plain',
        } },
    ]
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
