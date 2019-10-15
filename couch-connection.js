"use strict";

var nano = require("nano")("http://localhost:5984"),
  db = nano.use("tcc");
const util = require("util");
const uuidV4 = require("uuid/v4");

const promisifiedBulk = util.promisify(db.bulk);

let registrosCriados = 0;
var begin = Date.now();

const mapaCidades = new Map();
const mapaPessoas = new Map();

module.exports = {
  criarRegistros(data, primeiraImportacao) {
    if (!data || data.length === 0) {
      return;
    }

    const pessoas = [];
    const cidades = [];
    const saques = [];

    data.map(linha => {
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

      let cidadeId, pessoaId;

      if (mapaCidades.has(codigoMunicipio)) {
        cidadeId = mapaCidades.get(codigoMunicipio);
      } else {
        cidadeId = uuidV4();
        cidades.push({
          _id: cidadeId,
          nome: nomeMunicipio,
          codigo: codigoMunicipio,
          uf: sigla,
          tipo: "Cidade"
        });
        mapaCidades.set(codigoMunicipio, cidadeId);
      }
      if (mapaPessoas.has(nisFavorecido)) {
        pessoaId = mapaPessoas.get(nisFavorecido);
      } else {
        pessoaId = uuidV4();
        pessoas.push({
          _id: pessoaId,
          nome: nomeFavorecido,
          nis: nisFavorecido,
          tipo: "Pessoa"
        });

        mapaPessoas.set(nisFavorecido, pessoaId);
      }

      saques.push({
        favorecido: pessoaId,
        municipio: cidadeId,
        mesReferencia,
        mesCompetencia,
        data,
        valor
      });
    });

    const registros = [...pessoas, ...cidades, ...saques];

    promisifiedBulk({
      docs: registros

    }).then(() => {

      var end = Date.now();
      var timeSpent = (end - begin) / 1000 + "secs";
      registrosCriados += registros.length;

      console.log(
        `Registros criados ${registrosCriados} - Tempo gasto ${timeSpent}`
      );
    })
      .catch(err => console.log(err))
  }
}
