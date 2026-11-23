'use strict';

const { Readable, Writable } = require('stream');

const StreamSearch = require('streamsearch');

const {
  basename,
  convertToUTF8,
  getDecoder,
  parseContentType,
  parseDisposition,
} = require('../utils.js');

const BUF_CRLF = Buffer.from('\r\n');
const BUF_CR = Buffer.from('\r');
const BUF_DASH = Buffer.from('-');

function noop() {}

const MAX_HEADER_PAIRS = 2000; // From node
const MAX_HEADER_SIZE = 16 * 1024; // From node (its default value)

const HPARSER_NAME = 0;
const HPARSER_PRE_OWS = 1;
const HPARSER_VALUE = 2;
class HeaderParser {
  constructor(cb) {
    this.header = Object.create(null);
    this.pairCount = 0;
    this.byteCount = 0;
    this.state = HPARSER_NAME;
    this.name = '';
    this.value = '';
    this.crlf = 0;
    this.cb = cb;
  }

  reset() {
    this.header = Object.create(null);
    this.pairCount = 0;
    this.byteCount = 0;
    this.state = HPARSER_NAME;
    this.name = '';
    this.value = '';
    this.crlf = 0;
  }

  push(chunk, pos, end) {
    let start = pos;
    while (pos < end) {
      switch (this.state) {
        case HPARSER_NAME: {
          let done = false;
          for (; pos < end; ++pos) {
            if (this.byteCount === MAX_HEADER_SIZE)
              return -1;
            ++this.byteCount;
            const code = chunk[pos];
            if (TOKEN[code] !== 1) {
              if (code !== 58/* ':' */)
                return -1;
              this.name += chunk.latin1Slice(start, pos);
              if (this.name.length === 0)
                return -1;
              ++pos;
              done = true;
              this.state = HPARSER_PRE_OWS;
              break;
            }
          }
          if (!done) {
            this.name += chunk.latin1Slice(start, pos);
            break;
          }
          // FALLTHROUGH
        }
        case HPARSER_PRE_OWS: {
          // Skip optional whitespace
          let done = false;
          for (; pos < end; ++pos) {
            if (this.byteCount === MAX_HEADER_SIZE)
              return -1;
            ++this.byteCount;
            const code = chunk[pos];
            if (code !== 32/* ' ' */ && code !== 9/* '\t' */) {
              start = pos;
              done = true;
              this.state = HPARSER_VALUE;
              break;
            }
          }
          if (!done)
            break;
          // FALLTHROUGH
        }
        case HPARSER_VALUE:
          switch (this.crlf) {
            case 0: // Nothing yet
              for (; pos < end; ++pos) {
                if (this.byteCount === MAX_HEADER_SIZE)
                  return -1;
                ++this.byteCount;
                const code = chunk[pos];
                if (FIELD_VCHAR[code] !== 1) {
                  if (code !== 13/* '\r' */)
                    return -1;
                  ++this.crlf;
                  break;
                }
              }
              this.value += chunk.latin1Slice(start, pos++);
              break;
            case 1: // Received CR
              if (this.byteCount === MAX_HEADER_SIZE)
                return -1;
              ++this.byteCount;
              if (chunk[pos++] !== 10/* '\n' */)
                return -1;
              ++this.crlf;
              break;
            case 2: { // Received CR LF
              if (this.byteCount === MAX_HEADER_SIZE)
                return -1;
              ++this.byteCount;
              const code = chunk[pos];
              if (code === 32/* ' ' */ || code === 9/* '\t' */) {
                // Folded value
                start = pos;
                this.crlf = 0;
              } else {
                if (++this.pairCount < MAX_HEADER_PAIRS) {
                  this.name = this.name.toLowerCase();
                  if (this.header[this.name] === undefined)
                    this.header[this.name] = [this.value];
                  else
                    this.header[this.name].push(this.value);
                }
                if (code === 13/* '\r' */) {
                  ++this.crlf;
                  ++pos;
                } else {
                  // Assume start of next header field name
                  start = pos;
                  this.crlf = 0;
                  this.state = HPARSER_NAME;
                  this.name = '';
                  this.value = '';
                }
              }
              break;
            }
            case 3: { // Received CR LF CR
              if (this.byteCount === MAX_HEADER_SIZE)
                return -1;
              ++this.byteCount;
              if (chunk[pos++] !== 10/* '\n' */)
                return -1;
              // End of header
              const header = this.header;
              this.reset();
              this.cb(header);
              return pos;
            }
          }
          break;
      }
    }

    return pos;
  }
}

class FileStream extends Readable {
  constructor(opts, owner) {
    super(opts);
    this.truncated = false;
    this._readcb = null;
    this._done = false;
    const done = () => {
      if (this._done)
        return;
      this._done = true;
      // We need to make sure that we call any outstanding _writecb() that is
      // associated with this file so that processing of the rest of the form
      // can continue. This may not happen if the file stream ends right after
      // backpressure kicks in, so we force it here.
      this._read();
      if (--owner._fileEndsLeft === 0 && owner._finalcb) {
        const cb = owner._finalcb;
        owner._finalcb = null;
        // Make sure other 'end' event handlers get a chance to be executed
        // before busboy's 'finish' event is emitted
        process.nextTick(cb);
      }
    };
    // A file stream that is destroyed before it ends (for example because
    // decoding its content failed or the consumer aborted it) never emits
    // 'end', so make sure we still account for it when it closes.
    this.once('end', done);
    this.once('close', done);
  }
  _read(n) {
    const cb = this._readcb;
    if (cb) {
      this._readcb = null;
      cb();
    }
  }
}

const ignoreData = {
  push: (chunk, pos) => {},
  destroy: () => {},
};

function callAndUnsetCb(self, err) {
  const cb = self._writecb;
  self._writecb = null;
  if (err)
    self.destroy(err);
  else if (cb)
    cb();
}

function nullDecoder(val, hint) {
  return val;
}

const EMPTY_BUFFER = Buffer.alloc(0);

function getBase64Value(code) {
  // A-Z
  if (code >= 65 && code <= 90)
    return code - 65;
  // a-z
  if (code >= 97 && code <= 122)
    return code - 71;
  // 0-9
  if (code >= 48 && code <= 57)
    return code + 4;
  if (code === 43/* '+' */)
    return 62;
  if (code === 47/* '/' */)
    return 63;
  return -1;
}

function decodeBase64Group(group) {
  const a = getBase64Value(group.charCodeAt(0));
  const b = getBase64Value(group.charCodeAt(1));
  if (a === -1 || b === -1)
    throw new Error('Invalid base64 data');

  const c = group.charCodeAt(2);
  if (c === 61/* '=' */) {
    if (group.charCodeAt(3) !== 61/* '=' */)
      throw new Error('Invalid base64 data');
    return Buffer.from([(a << 2) | (b >> 4)]);
  }

  const cv = getBase64Value(c);
  if (cv === -1)
    throw new Error('Invalid base64 data');

  const d = group.charCodeAt(3);
  if (d === 61/* '=' */) {
    return Buffer.from([
      (a << 2) | (b >> 4),
      ((b & 15) << 4) | (cv >> 2),
    ]);
  }

  const dv = getBase64Value(d);
  if (dv === -1)
    throw new Error('Invalid base64 data');
  return Buffer.from([
    (a << 2) | (b >> 4),
    ((b & 15) << 4) | (cv >> 2),
    ((cv & 3) << 6) | dv,
  ]);
}

// Decodes the body of a single MIME part according to its
// Content-Transfer-Encoding. A separate instance is created for every part so
// that no decoder state can leak into a subsequent part.
class PartDecoder {
  constructor(encoding) {
    this.encoding = encoding;
    this.pending = '';
    this.padded = false;
    switch (encoding) {
      case '':
      case '7bit':
      case '8bit':
      case 'binary':
        this.type = 'identity';
        break;
      case 'base64':
        this.type = 'base64';
        break;
      default:
        this.type = 'unsupported';
    }
  }

  get supported() {
    return this.type !== 'unsupported';
  }

  write(chunk) {
    switch (this.type) {
      case 'identity':
        return chunk;
      case 'base64':
        return this._writeBase64(chunk);
      default:
        throw new Error(
          `Unsupported content transfer encoding: ${this.encoding}`
        );
    }
  }

  end() {
    switch (this.type) {
      case 'identity':
        return EMPTY_BUFFER;
      case 'base64':
        return this._endBase64();
      default:
        throw new Error(
          `Unsupported content transfer encoding: ${this.encoding}`
        );
    }
  }

  _writeBase64(chunk) {
    const text = chunk.toString('latin1');
    let data = this.pending;

    for (let i = 0; i < text.length; ++i) {
      const code = text.charCodeAt(i);
      if (code === 32/* ' ' */ || code === 9/* '\t' */ || code === 13/* '\r' */
          || code === 10/* '\n' */) {
        continue;
      }
      if (this.padded)
        throw new Error('Invalid base64 data');
      data += text[i];
    }

    if (data.length === 0)
      return EMPTY_BUFFER;

    const out = [];
    let i = 0;
    for (; i + 4 <= data.length; i += 4) {
      const group = data.slice(i, i + 4);
      out.push(decodeBase64Group(group));
      if (group.charCodeAt(2) === 61/* '=' */
          || group.charCodeAt(3) === 61/* '=' */) {
        this.padded = true;
        if (i + 4 < data.length)
          throw new Error('Invalid base64 data');
      }
    }

    this.pending = data.slice(i);
    if (this.padded && this.pending.length !== 0)
      throw new Error('Invalid base64 data');

    return (out.length === 0 ? EMPTY_BUFFER : Buffer.concat(out));
  }

  _endBase64() {
    const pending = this.pending;
    this.pending = '';
    if (pending.length === 0)
      return EMPTY_BUFFER;
    if (this.padded || pending.length === 1
        || pending.charCodeAt(pending.length - 1) === 61/* '=' */) {
      throw new Error('Invalid base64 data');
    }

    const a = getBase64Value(pending.charCodeAt(0));
    const b = getBase64Value(pending.charCodeAt(1));
    if (a === -1 || b === -1)
      throw new Error('Invalid base64 data');

    // Accept unpadded base64 for compatibility with senders that omit the
    // trailing '=' characters.
    if (pending.length === 2)
      return Buffer.from([(a << 2) | (b >> 4)]);

    const c = getBase64Value(pending.charCodeAt(2));
    if (c === -1)
      throw new Error('Invalid base64 data');
    return Buffer.from([
      (a << 2) | (b >> 4),
      ((b & 15) << 4) | (c >> 2),
    ]);
  }
}

class Multipart extends Writable {
  constructor(cfg) {
    const streamOpts = {
      autoDestroy: true,
      emitClose: true,
      highWaterMark: (typeof cfg.highWaterMark === 'number'
                      ? cfg.highWaterMark
                      : undefined),
    };
    super(streamOpts);

    if (!cfg.conType.params || typeof cfg.conType.params.boundary !== 'string')
      throw new Error('Multipart: Boundary not found');

    const boundary = cfg.conType.params.boundary;
    const paramDecoder = (typeof cfg.defParamCharset === 'string'
                            && cfg.defParamCharset
                          ? getDecoder(cfg.defParamCharset)
                          : nullDecoder);
    const defCharset = (cfg.defCharset || 'utf8');
    const preservePath = cfg.preservePath;
    const fileOpts = {
      autoDestroy: true,
      emitClose: true,
      highWaterMark: (typeof cfg.fileHwm === 'number'
                      ? cfg.fileHwm
                      : undefined),
    };

    const limits = cfg.limits;
    const fieldSizeLimit = (limits && typeof limits.fieldSize === 'number'
                            ? limits.fieldSize
                            : 1 * 1024 * 1024);
    const fileSizeLimit = (limits && typeof limits.fileSize === 'number'
                           ? limits.fileSize
                           : Infinity);
    const filesLimit = (limits && typeof limits.files === 'number'
                        ? limits.files
                        : Infinity);
    const fieldsLimit = (limits && typeof limits.fields === 'number'
                         ? limits.fields
                         : Infinity);
    const partsLimit = (limits && typeof limits.parts === 'number'
                        ? limits.parts
                        : Infinity);

    let parts = -1; // Account for initial boundary
    let fields = 0;
    let files = 0;
    let skipPart = false;

    this._fileEndsLeft = 0;
    this._fileStream = undefined;
    this._complete = false;
    let fileSize = 0;

    let field;
    let fieldSize = 0;
    let partCharset;
    let partEncoding;
    let partType;
    let partName;
    let partTruncated = false;
    let partDecoder;

    let hitFilesLimit = false;
    let hitFieldsLimit = false;

    this._hparser = null;
    const hparser = new HeaderParser((header) => {
      this._hparser = null;
      skipPart = false;

      partType = 'text/plain';
      partCharset = defCharset;
      partEncoding = '7bit';
      partName = undefined;
      partTruncated = false;
      partDecoder = undefined;
      field = undefined;
      fieldSize = 0;

      let filename;
      if (!header['content-disposition']) {
        skipPart = true;
        return;
      }

      const disp = parseDisposition(header['content-disposition'][0],
                                    paramDecoder);
      if (!disp || disp.type !== 'form-data') {
        skipPart = true;
        return;
      }

      if (disp.params) {
        if (disp.params.name)
          partName = disp.params.name;

        if (disp.params['filename*'])
          filename = disp.params['filename*'];
        else if (disp.params.filename)
          filename = disp.params.filename;

        if (filename !== undefined && !preservePath)
          filename = basename(filename);
      }

      if (header['content-type']) {
        const conType = parseContentType(header['content-type'][0]);
        if (conType) {
          partType = `${conType.type}/${conType.subtype}`;
          if (conType.params && typeof conType.params.charset === 'string')
            partCharset = conType.params.charset.toLowerCase();
        }
      }

      if (header['content-transfer-encoding'])
        partEncoding = header['content-transfer-encoding'][0].toLowerCase();

      partDecoder = new PartDecoder(partEncoding.trim());
      if (!partDecoder.supported) {
        skipPart = true;
        this.emit(
          'error',
          new Error(`Unsupported content transfer encoding: ${partEncoding}`)
        );
        return;
      }

      if (partType === 'application/octet-stream' || filename !== undefined) {
        // File

        if (files === filesLimit) {
          if (!hitFilesLimit) {
            hitFilesLimit = true;
            this.emit('filesLimit');
          }
          skipPart = true;
          return;
        }
        ++files;

        if (this.listenerCount('file') === 0) {
          skipPart = true;
          return;
        }

        fileSize = 0;
        const fileStream = new FileStream(fileOpts, this);
        this._fileStream = fileStream;
        ++this._fileEndsLeft;
        this.emit(
          'file',
          partName,
          fileStream,
          { filename,
            encoding: partEncoding,
            mimeType: partType }
        );
      } else {
        // Non-file

        if (fields === fieldsLimit) {
          if (!hitFieldsLimit) {
            hitFieldsLimit = true;
            this.emit('fieldsLimit');
          }
          skipPart = true;
          return;
        }
        ++fields;

        if (this.listenerCount('field') === 0) {
          skipPart = true;
          return;
        }

        field = [];
        fieldSize = 0;
      }
    });

    const appendFileData = (stream, chunk) => {
      const remaining = Math.max(0, fileSizeLimit - fileSize);
      const actualLen = Math.min(chunk.length, remaining);
      if (actualLen > 0) {
        fileSize += actualLen;
        const out = (actualLen === chunk.length
                     ? chunk
                     : chunk.slice(0, actualLen));
        if (!stream.push(out)) {
          if (this._writecb)
            stream._readcb = this._writecb;
          this._writecb = null;
        }
      }
      if (fileSize >= fileSizeLimit) {
        stream.emit('limit');
        stream.truncated = true;
        skipPart = true;
        return false;
      }
      return true;
    };

    const appendFieldData = (chunk) => {
      const remaining = Math.max(0, fieldSizeLimit - fieldSize);
      const actualLen = Math.min(chunk.length, remaining);
      if (actualLen > 0) {
        field.push(actualLen === chunk.length
                   ? chunk
                   : chunk.slice(0, actualLen));
        fieldSize += actualLen;
      }
      if (actualLen < chunk.length) {
        skipPart = true;
        partTruncated = true;
      }
    };

    let matchPostBoundary = 0;
    const ssCb = (isMatch, data, start, end, isDataSafe) => {
retrydata:
      while (data) {
        if (this._hparser !== null) {
          const ret = this._hparser.push(data, start, end);
          if (ret === -1) {
            this._hparser = null;
            hparser.reset();
            this.emit('error', new Error('Malformed part header'));
            break;
          }
          start = ret;
        }

        if (start === end)
          break;

        if (matchPostBoundary !== 0) {
          if (matchPostBoundary === 1) {
            switch (data[start]) {
              case 45: // '-'
                // Try matching '--' after boundary
                matchPostBoundary = 2;
                ++start;
                break;
              case 13: // '\r'
                // Try matching CR LF before header
                matchPostBoundary = 3;
                ++start;
                break;
              default:
                matchPostBoundary = 0;
            }
            if (start === end)
              return;
          }

          if (matchPostBoundary === 2) {
            matchPostBoundary = 0;
            if (data[start] === 45/* '-' */) {
              // End of multipart data
              this._complete = true;
              this._bparser = ignoreData;
              return;
            }
            // We saw something other than '-', so put the dash we consumed
            // "back"
            const writecb = this._writecb;
            this._writecb = noop;
            ssCb(false, BUF_DASH, 0, 1, false);
            this._writecb = writecb;
          } else if (matchPostBoundary === 3) {
            matchPostBoundary = 0;
            if (data[start] === 10/* '\n' */) {
              ++start;
              if (parts >= partsLimit)
                break;
              // Prepare the header parser
              this._hparser = hparser;
              if (start === end)
                break;
              // Process the remaining data as a header
              continue retrydata;
            } else {
              // We saw something other than LF, so put the CR we consumed
              // "back"
              const writecb = this._writecb;
              this._writecb = noop;
              ssCb(false, BUF_CR, 0, 1, false);
              this._writecb = writecb;
            }
          }
        }

        if (!skipPart) {
          if (this._fileStream) {
            let chunk;
            const rawLen = end - start;
            if (!isDataSafe) {
              chunk = Buffer.allocUnsafe(rawLen);
              data.copy(chunk, 0, start, start + rawLen);
            } else {
              chunk = data.slice(start, start + rawLen);
            }

            try {
              chunk = (partDecoder ? partDecoder.write(chunk) : chunk);
            } catch (err) {
              const stream = this._fileStream;
              this._fileStream = null;
              stream.destroy(err);
              skipPart = true;
              break;
            }

            if (!appendFileData(this._fileStream, chunk))
              break;
          } else if (field !== undefined) {
            let chunk;
            const rawLen = end - start;
            if (!isDataSafe) {
              chunk = Buffer.allocUnsafe(rawLen);
              data.copy(chunk, 0, start, start + rawLen);
            } else {
              chunk = data.slice(start, start + rawLen);
            }

            try {
              chunk = (partDecoder ? partDecoder.write(chunk) : chunk);
            } catch (err) {
              field = undefined;
              fieldSize = 0;
              skipPart = true;
              this.emit('error', err);
              break;
            }

            appendFieldData(chunk);
          }
        }

        break;
      }

      if (isMatch) {
        matchPostBoundary = 1;

        if (this._fileStream) {
          // End the active file stream if the previous part was a file
          const stream = this._fileStream;
          this._fileStream = null;
          if (!skipPart && partDecoder) {
            try {
              const chunk = partDecoder.end();
              if (chunk.length !== 0)
                appendFileData(stream, chunk);
            } catch (err) {
              stream.destroy(err);
            }
          }
          if (!stream.destroyed)
            stream.push(null);
        } else if (field !== undefined) {
          if (!skipPart && partDecoder) {
            try {
              const chunk = partDecoder.end();
              if (chunk.length !== 0)
                appendFieldData(chunk);
            } catch (err) {
              field = undefined;
              fieldSize = 0;
              skipPart = true;
              this.emit('error', err);
            }
          }
          if (field !== undefined) {
            let data;
            switch (field.length) {
              case 0:
                data = '';
                break;
              case 1:
                data = convertToUTF8(field[0], partCharset, 0);
                break;
              default:
                data = convertToUTF8(
                  Buffer.concat(field, fieldSize),
                  partCharset,
                  0
                );
            }
            field = undefined;
            fieldSize = 0;
            this.emit(
              'field',
              partName,
              data,
              { nameTruncated: false,
                valueTruncated: partTruncated,
                encoding: partEncoding,
                mimeType: partType }
            );
          }
        }

        if (++parts === partsLimit)
          this.emit('partsLimit');
      }
    };
    this._bparser = new StreamSearch(`\r\n--${boundary}`, ssCb);

    this._writecb = null;
    this._finalcb = null;

    // Just in case there is no preamble
    this.write(BUF_CRLF);
  }

  static detect(conType) {
    return (conType.type === 'multipart' && conType.subtype === 'form-data');
  }

  _write(chunk, enc, cb) {
    this._writecb = cb;
    this._bparser.push(chunk, 0);
    if (this._writecb)
      callAndUnsetCb(this);
  }

  _destroy(err, cb) {
    this._hparser = null;
    this._bparser = ignoreData;
    if (!err)
      err = checkEndState(this);
    const fileStream = this._fileStream;
    if (fileStream) {
      this._fileStream = null;
      fileStream.destroy(err);
    }
    cb(err);
  }

  _final(cb) {
    this._bparser.destroy();
    if (!this._complete)
      return cb(new Error('Unexpected end of form'));
    if (this._fileEndsLeft)
      this._finalcb = finalcb.bind(null, this, cb);
    else
      finalcb(this, cb);
  }
}

function finalcb(self, cb, err) {
  if (err)
    return cb(err);
  err = checkEndState(self);
  cb(err);
}

function checkEndState(self) {
  if (self._hparser)
    return new Error('Malformed part header');
  const fileStream = self._fileStream;
  if (fileStream) {
    self._fileStream = null;
    fileStream.destroy(new Error('Unexpected end of file'));
  }
  if (!self._complete)
    return new Error('Unexpected end of form');
}

const TOKEN = [
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 1, 0, 1, 1, 1, 1, 1, 0, 0, 1, 1, 0, 1, 1, 0,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,
  0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

const FIELD_VCHAR = [
  0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
];

module.exports = Multipart;
