"use strict";

var nano = require("nano")("http://localhost:5984"),
  db = nano.use("tcc");

const uuidV4 = require("uuid/v4");
let saquesCriados = 0;
let pessoasCriadas = 0;
let cidadesCriadas = 0;
var begin = Date.now();

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

      const pessoaId = uuidV4();
      const cidadeId = uuidV4();

      pessoas.push({
        _id: pessoaId,
        nome: nomeFavorecido,
        nis: nisFavorecido,
        tipo: "Pessoa"
      });

      cidades.push({
        _id: cidadeId,
        nome: nomeMunicipio,
        codigo: codigoMunicipio,
        uf: sigla,
        tipo: "Cidade"
      });

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
  
    db.bulk(
      {
        docs: registros
      },
      (err, data) => {
        if (err) {
          throw new Error(err);
        }

        saquesCriados += saques.length;
        pessoasCriadas += pessoas.length;
        cidadesCriadas += cidades.length;

        var end = Date.now();
        var timeSpent = (end - begin) / 1000 + "secs";

        console.log(
          `Saques criados: ${saquesCriados} - Pessoas criadas: ${pessoasCriadas} - Cidades criadas: ${cidadesCriadas} - Tempo gasto ${timeSpent}`
        );
      }
    );
  }
};
