'use strict';

const assert = require('assert');
const { inspect } = require('util');

const { mustCall } = require(`${__dirname}/common.js`);

const busboy = require('..');

const BOUNDARY = '----charsetConflictBoundary';
const CRLF = Buffer.from('\r\n');
const OPEN = Buffer.from(`--${BOUNDARY}\r\n`);
const CLOSE = Buffer.from(`--${BOUNDARY}--\r\n`);

// The same bytes are used for both the parameter name and the field value so
// that any difference in their decoding is visible in the results.
const CAFE_BYTES = Buffer.from([0x63, 0x61, 0x66, 0xC3, 0xA9]);
const UTF8_CAFE = 'caf\u00e9';
const LATIN1_CAFE = 'caf\u00c3\u00a9';

function makePart(opts) {
  const disposition = [
    Buffer.from('Content-Disposition: form-data; name="', 'latin1'),
    CAFE_BYTES,
    Buffer.from('"', 'latin1'),
  ];

  if (opts.filename === true) {
    disposition.push(
      Buffer.from('; filename="', 'latin1'),
      CAFE_BYTES,
      Buffer.from('"', 'latin1')
    );
  }

  const chunks = [OPEN, Buffer.concat(disposition), CRLF];
  if (opts.charset !== undefined) {
    chunks.push(
      Buffer.from(
        `Content-Type: ${opts.mimeType || 'text/plain'}; `
          + `charset=${opts.charset}`
      ),
      CRLF
    );
  } else if (opts.mimeType !== undefined) {
    chunks.push(Buffer.from(`Content-Type: ${opts.mimeType}`), CRLF);
  }
  chunks.push(CRLF, CAFE_BYTES, CRLF);
  return Buffer.concat(chunks);
}

function makeBody(parts) {
  return Buffer.concat([...parts.map(makePart), CLOSE]);
}

function parse(cfg, body) {
  return new Promise((resolve) => {
    const bb = busboy({
      limits: {},
      defCharset: cfg.defCharset,
      defParamCharset: cfg.defParamCharset,
      headers: {
        'content-type': `multipart/form-data; boundary=${BOUNDARY}`,
      },
    });
    const results = [];

    bb.on('field', (name, val, info) => {
      results.push({
        type: 'field',
        name,
        val,
        encoding: info.encoding,
        mimeType: info.mimeType,
      });
    });
    bb.on('file', (name, stream, info) => {
      const file = {
        type: 'file',
        name,
        filename: info.filename,
        data: null,
        encoding: info.encoding,
        mimeType: info.mimeType,
      };
      results.push(file);
      const chunks = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('end', () => {
        file.data = Buffer.concat(chunks);
      });
    });
    bb.on('error', (err) => results.push({ error: err.message }));
    bb.on('close', mustCall(() => resolve(results)));
    bb.end(body);
  });
}

const FIELD = (name, val) => ({
  type: 'field',
  name,
  val,
  encoding: '7bit',
  mimeType: 'text/plain',
});

const FILE = (name, filename) => ({
  type: 'file',
  name,
  filename,
  data: CAFE_BYTES,
  encoding: '7bit',
  mimeType: 'application/octet-stream',
});

const cases = [
  {
    what: 'Part charset applies to parameter names and field values',
    cfg: {},
    parts: [
      { charset: 'utf-8' },
      { charset: 'iso-8859-1' },
      { charset: undefined },
      { charset: 'utf-8' },
      { charset: 'iso-8859-1' },
    ],
    expected: [
      FIELD(UTF8_CAFE, UTF8_CAFE),
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
      FIELD(LATIN1_CAFE, UTF8_CAFE),
      FIELD(UTF8_CAFE, UTF8_CAFE),
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
    ],
  },
  {
    what: 'Part charset wins over conflicting defParamCharset/defCharset',
    cfg: { defParamCharset: 'utf8', defCharset: 'latin1' },
    parts: [
      { charset: 'utf-8' },
      { charset: 'latin1' },
      { charset: undefined },
      { charset: 'utf-8' },
      { charset: 'latin1' },
    ],
    expected: [
      FIELD(UTF8_CAFE, UTF8_CAFE),
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
      FIELD(UTF8_CAFE, LATIN1_CAFE),
      FIELD(UTF8_CAFE, UTF8_CAFE),
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
    ],
  },
  {
    what: 'Empty part charset falls back to configured defaults',
    cfg: { defParamCharset: 'utf8', defCharset: 'latin1' },
    parts: [
      { charset: '""' },
      { charset: 'utf-8' },
      { charset: '""' },
      { charset: 'latin1' },
    ],
    expected: [
      FIELD(UTF8_CAFE, LATIN1_CAFE),
      FIELD(UTF8_CAFE, UTF8_CAFE),
      FIELD(UTF8_CAFE, LATIN1_CAFE),
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
    ],
  },
  {
    what: 'Unknown part charset skips only the affected part',
    cfg: {},
    parts: [
      { charset: 'utf-8' },
      { charset: 'x-unknown-charset' },
      { charset: 'iso-8859-1' },
      { charset: 'x-unknown-charset' },
      { charset: 'utf-8' },
    ],
    expected: [
      FIELD(UTF8_CAFE, UTF8_CAFE),
      { error: 'Unsupported charset: x-unknown-charset' },
      FIELD(LATIN1_CAFE, LATIN1_CAFE),
      { error: 'Unsupported charset: x-unknown-charset' },
      FIELD(UTF8_CAFE, UTF8_CAFE),
    ],
  },
  {
    what: 'File parameter names and filenames use the part charset',
    cfg: {},
    parts: [
      { charset: 'utf-8', mimeType: 'application/octet-stream',
        filename: true },
      { charset: 'iso-8859-1', mimeType: 'application/octet-stream',
        filename: true },
    ],
    expected: [
      FILE(UTF8_CAFE, UTF8_CAFE),
      FILE(LATIN1_CAFE, LATIN1_CAFE),
    ],
  },
];

(async () => {
  for (const prop of ['defCharset', 'defParamCharset']) {
    assert.throws(
      () => busboy({
        [prop]: 'x-unknown-charset',
        headers: {
          'content-type': `multipart/form-data; boundary=${BOUNDARY}`,
        },
      }),
      /Unsupported charset: x-unknown-charset/
    );
  }

  for (const test of cases) {
    const results = await parse(test.cfg, makeBody(test.parts));
    assert.deepStrictEqual(
      results,
      test.expected,
      `[${test.what}] Results mismatch.\n`
        + `Parsed: ${inspect(results)}\n`
        + `Expected: ${inspect(test.expected)}`
    );
  }
})().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
