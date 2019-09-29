"use strict";

const mongoose = require("mongoose");
const { Schema } = mongoose;
const { ObjectId } = mongoose.Types;

const SaqueSchema = new Schema({
  favorecido: {
    type: ObjectId,
    ref: "Pessoa",
    required: true
  },
  municipio: {
    type: ObjectId,
    ref: "Cidade",
    required: true
  },
  mesReferencia: String,
  mesCompetencia: String,
  data: String,
  valor: Number
});

module.exports = mongoose.model("Saque", SaqueSchema, "saques");
