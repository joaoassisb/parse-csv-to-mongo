"use strict";
const fs = require("fs");
const mongoose = require("mongoose");
const csv = require("fast-csv");

const Cidade = require("./cidade.model");
const Pessoa = require("./pessoa.model");
const Saque = require("./saque.model");

const mapaPessoas = new Map();
const mapaCidades = new Map();

var begin = Date.now();
var end = Date.now();
var timeSpent = (end - begin) / 1000 + "secs";
var saquesCriados = 0;

const filePath = process.env.FILEPATH;

if (!filePath) {
  throw new Error(
    "É necessário definir o caminho do arquivo através da variável FILEPATH"
  );
}

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
      .on("data", function(data) {
        if (!data || !Array.isArray(data) || data[2] === "UF") {
          return;
        }

        return criarRegistroBanco(data);
      })
      .on("error", function(err) {
        console.log(err);
        process.exit(0);
      });
  })
  .catch(err => {
    console.log("Conexão com banco de dados falhou !", err);
    process.exit(0);
  });

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
  let pessoa, cidade;
  let promises = [];

  if (mapaPessoas.has(nomeFavorecido)) {
    pessoa = mapaPessoas.get(nomeFavorecido);
    promises.push(Promise.resolve(pessoa));
  } else {
    pessoa = new Pessoa({
      nome: nomeFavorecido,
      nis: nisFavorecido
    });

    promises.push(pessoa.loadOrCreate());
  }

  if (mapaCidades.has(codigoMunicipio)) {
    cidade = mapaCidades.get(codigoMunicipio);
    promises.push(Promise.resolve(cidade));
  } else {
    cidade = new Cidade({
      nome: nomeMunicipio,
      codigo: codigoMunicipio,
      uf: sigla
    });
    promises.push(cidade.loadOrCreate());
  }

  return Promise.all(promises)
    .then(([pessoa, cidade]) => {
      mapaPessoas.set(pessoa.nis, pessoa._id);
      mapaCidades.set(cidade.codigo, cidade._id);

      const saque = new Saque({
        favorecido: pessoa._id,
        municipio: cidade._id,
        mesCompetencia,
        mesCompetencia,
        data,
        valor: parseFloat(valor)
      });

      return saque.save();
    })
    .then(saque => {
      end = Date.now();
      timeSpent = (end - begin) / 1000 + "secs";
      saquesCriados += 1;
      console.log(
        `Saques criados: ${saquesCriados} - Tempo Gasto: ${timeSpent}`
      );
    })
    .catch(err => {
      console.log("Erro na criação de um registro", err);
    });
}
