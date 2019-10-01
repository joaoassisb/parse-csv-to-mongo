"use strict";

const mongoose = require("mongoose");
const { Schema } = mongoose;
const { ObjectId } = mongoose.Types;

const CidadeSchema = new Schema({
  nome: {
    type: String,
    required: true
  },
  codigo: {
    type: String,
    required: true
  },
  uf: {
    type: String,
    maxlength: 2,
    required: true
  }
});

CidadeSchema.index({ codigo: 1 });

CidadeSchema.method({
  loadOrCreate() {
    return this.model("Cidade")
      .findOne({ codigo: this.codigo })
      .then(pessoa => {
        if (pessoa) {
          return pessoa;
        }

        return this.save();
      });
  }
});

module.exports = mongoose.model("Cidade", CidadeSchema, "cidades");
