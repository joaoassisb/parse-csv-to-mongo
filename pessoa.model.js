"use strict";

const mongoose = require("mongoose");
const { Schema } = mongoose;

const PessoaSchema = new Schema({
  nome: {
    type: String,
    required: true
  },
  nis: {
    type: String,
    unique: true
  }
});

PessoaSchema.method({
  loadOrCreate() {
    return this.model("Pessoa")
      .findOne({ nis: this.nis })
      .then(pessoa => {
        if (pessoa) {
          return pessoa;
        }

        return this.save();
      });
  }
});

module.exports = mongoose.model("Pessoa", PessoaSchema, "pessoas");
