"use strict";

const { Transform } = require("stream");

const mongoose = require("mongoose");

const BUFFER_SIZE = 1000;

module.exports = {
  bufferizeDocuments() {
    let buffer = [];

    return new Transform({
      objectMode: true,
      transform(doc, encoding, next) {
        buffer.push(doc);
        if (buffer.length === BUFFER_SIZE) {
          this.push(buffer);
          buffer = [];
        }

        next();
      },
      flush(done) {
        if (buffer.length > 0) {
          this.push(buffer);
        }
        done();
      }
    });
  }
};
