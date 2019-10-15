"use strict";
const fs = require("fs");
const csv = require("fast-csv");

const { bufferizeDocuments } = require("./transformable-stream");
const {
  connectToMongo,
  criarRegistros: criarRegistrosMongo
} = require("./mongo-connection");
const { criarRegistros: criarRegistrosCouch } = require("./couch-connection");

const filePath = process.env.FILEPATH;
const firstImportation = process.env.FIRST;
const database = process.env.DB;

let dbConnection;

if (database === "mongo") {
  dbConnection = connectToMongo;
} else {
  dbConnection = Promise.resolve();
}

if (!filePath) {
  throw new Error(
    "É necessário definir o caminho do arquivo através da variável FILEPATH"
  );
}
const begin = Date.now()

dbConnection
  .then(() => {
    console.log("Conexão com banco de dados estabelecida com sucesso");
  })
  .then(() => {
    fs.createReadStream(filePath, {
      encoding: "utf8"
    })
      .pipe(csv.parse({ delimiter: ";" }))
      .pipe(bufferizeDocuments())
      .on("data", function (data) {
        if (database === "mongo") {
          criarRegistrosMongo(data, firstImportation);
        } else {
          criarRegistrosCouch(data, firstImportation);
        }
      })
      .on("end", function () {
        console.log("FINISHED READING");
      });
  })
  .catch(err => {
    console.log("Conexão com banco de dados falhou !", err);
    process.exit(0);
  });
