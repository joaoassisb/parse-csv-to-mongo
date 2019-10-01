"use strict";
const fs = require("fs");
const mongoose = require("mongoose");
const csv = require("fast-csv");

const { bufferizeDocuments } = require("./transformable-stream");

const Cidade = require("./cidade.model");
const Pessoa = require("./pessoa.model");
const Saque = require("./saque.model");

var begin = Date.now();
let saquesCriados = 0;
const filePath = process.env.FILEPATH;

if (!filePath) {
  throw new Error(
    "É necessário definir o caminho do arquivo através da variável FILEPATH"
  );
}

const mapaPessoas = new Map();
const mapaCidades = new Map();

mongoose
  .connect("mongodb://localhost:27017/tcc-mongo", {
    useNewUrlParser: true
  })
  .then(() => {
    console.log("Conexão com banco de dados estabelecida com sucesso");
    fs.createReadStream(filePath, {
      encoding: "utf8"
    })
      .pipe(csv.parse({ delimiter: ";" }))
      .pipe(bufferizeDocuments())
      .on("data", function(data) {
        criarRegistros(data);
      })
      .on("end", function() {
        console.log("FINISHED READING");
      });

    //   .on("data", function(data) {
    //     if (!data || !Array.isArray(data) || data[2] === "UF") {
    //       return;
    //     }

    //     return criarRegistroBanco(data);
    //   })
    //   .on("error", function(err) {
    //     console.log(err);
    //     process.exit(0);
    //   });
  })
  .catch(err => {
    console.log("Conexão com banco de dados falhou !", err);
    process.exit(0);
  });

function criarRegistros(data) {
  let pessoas = [];
  let cidades = [];
  let saques = [];

  data.forEach(linha => {
    const [
      mesReferencia,
      mesCompetencia,
      sigla,
      codigoMunicipio,
      nomeMunicipio,
      nisFavorecido,
      nomeFavorecido,
      data,
      valor
    ] = linha;

    let cidade;

    if (mapaCidades.has(codigoMunicipio)) {
      cidade = mapaCidades.get(codigoMunicipio);
    } else {
      cidade = new Cidade({
        nome: nomeMunicipio,
        codigo: codigoMunicipio,
        uf: sigla
      });

      mapaCidades.set(codigoMunicipio, cidade);
      cidades.push(cidade);
    }

    const pessoa = new Pessoa({
      nome: nomeFavorecido,
      nis: nisFavorecido
    });

    const saque = new Saque({
      favorecido: pessoa._id,
      municipio: cidade._id,
      mesCompetencia,
      mesCompetencia,
      data,
      valor: parseFloat(valor)
    });

    saques.push(saque);
  });

  var end = Date.now();
  var timeSpent = (end - begin) / 1000 + "secs";

  console.log(
    `Saques criados: ${(saquesCriados +=
      saques.length)} - Tempo gasto ${timeSpent}`
  );

  Pessoa.insertMany(pessoas);
  Cidade.insertMany(cidades);
  Saque.insertMany(saques);
}

function criarRegistroBanco([
  mesReferencia,
  mesCompetencia,
  sigla,
  codigoMunicipio,
  nomeMunicipio,
  nisFavorecido,
  nomeFavorecido,
  data,
  valor
]) {
  const pessoa = new Pessoa({
    nome: nomeFavorecido,
    nis: nisFavorecido
  });

  const cidade = new Cidade({
    nome: nomeMunicipio,
    codigo: codigoMunicipio,
    uf: sigla
  });

  const saque = new Saque({
    favorecido: pessoa._id,
    municipio: cidade._id,
    mesCompetencia,
    mesCompetencia,
    data,
    valor: parseFloat(valor)
  });

  pessoa.loadOrCreate();
  cidade.loadOrCreate();
  saque.save();
}
