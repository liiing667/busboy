'use strict';

const assert = require('assert');

const busboy = require('..');
const Multipart = require('../lib/types/multipart.js');
const { parseContentType } = require('../lib/utils.js');

function makeBody(boundary, fields) {
  let body = '';
  for (const [name, value] of fields) {
    body += `--${boundary}\r\n`
          + `Content-Disposition: form-data; name="${name}"\r\n`
          + '\r\n'
          + `${value}\r\n`;
  }
  return body + `--${boundary}--\r\n`;
}

function parseWith(kind, header, body) {
  const conType = parseContentType(header);
  const parser = (kind === 'factory'
                  ? busboy({ headers: { 'content-type': header } })
                  : new Multipart({
                    headers: { 'content-type': header },
                    conType,
                  }));

  return new Promise((resolve) => {
    const result = { fields: [], errors: [] };
    parser.on('field', (name, value) => {
      result.fields.push([name, value]);
    });
    parser.on('error', (err) => {
      result.errors.push(err.message);
    });
    parser.on('close', () => resolve(result));
    parser.end(body);
  });
}

async function main() {
  const fields = [['one', 'first value'], ['two', 'second value']];
  const cases = [
    {
      name: 'quoted boundary',
      header: 'multipart/form-data; boundary="Quoted-Case-123"',
      boundary: 'Quoted-Case-123',
    },
    {
      name: 'duplicate boundary',
      header: 'multipart/form-data; boundary=first; boundary=Last-Case-456',
      boundary: 'Last-Case-456',
    },
    {
      name: 'parameter name case',
      header: 'MULTIPART/FORM-DATA; BOUNDARY="CaseSensitive-789"',
      boundary: 'CaseSensitive-789',
    },
  ];

  for (const test of cases) {
    const conType = parseContentType(test.header);
    assert.strictEqual(conType.params.boundary, test.boundary, test.name);
    assert.strictEqual(Multipart.detect(conType), true, test.name);

    const body = makeBody(test.boundary, fields);
    const viaFactory = await parseWith('factory', test.header, body);
    const viaInstance = await parseWith('instance', test.header, body);

    assert.deepStrictEqual(viaFactory, {
      fields,
      errors: [],
    }, test.name);
    assert.deepStrictEqual(viaInstance, viaFactory, test.name);
  }

  for (const header of [
    'multipart/form-data; boundary=""',
    'multipart/form-data; boundary=',
    'multipart/form-data; boundary',
    'multipart/form-data; boundary="   "',
  ]) {
    assert.strictEqual(
      Multipart.detect(parseContentType(header)),
      false,
      header
    );
    assert.throws(
      () => busboy({ headers: { 'content-type': header } }),
      /Multipart: Boundary not found/
    );
    assert.throws(
      () => new Multipart({
        headers: { 'content-type': header },
        conType: parseContentType(header),
      }),
      /Multipart: Boundary not found/
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
