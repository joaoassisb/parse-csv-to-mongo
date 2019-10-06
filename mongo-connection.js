"use strict";

const mongoose = require("mongoose");
let saquesCriados = 0;
let pessoasCriadas = 0;
let cidadesCriadas = 0;
var begin = Date.now();

const mapaCidades = new Map();
const mapaPessoas = new Map();

module.exports = {
  connectToMongo() {
    return mongoose.connect("mongodb://localhost:27017/tcc-mongo", {
      useNewUrlParser: true
    });
  },
  criarRegistros(data, primeiraImportacao) {
    if (primeiraImportacao) {
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

        let cidade, pessoa;

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

        if (mapaPessoas.has(nisFavorecido)) {
          pessoa = mapaPessoas.get(nisFavorecido);
        } else {
          pessoa = new Pessoa({
            nome: nomeFavorecido,
            nis: nisFavorecido
          });

          pessoas.push(pessoa);
        }

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

      insertCidades(cidades);
      insertPessoas(pessoas);
      insertSaques(saques);
    } else {
      Promise.all(
        data.map(
          ([
            mesReferencia,
            mesCompetencia,
            sigla,
            codigoMunicipio,
            nomeMunicipio,
            nisFavorecido,
            nomeFavorecido,
            data,
            valor
          ]) =>
            Promise.all([
              getCidade({ codigo: codigoMunicipio }),
              getPessoa({ nis: nisFavorecido })
            ]).then(([cidade, pessoa]) => {
              let newPessoa = {};

              if (!pessoa._id) {
                newPessoa = new Pessoa({
                  nome: nomeFavorecido,
                  nis: nisFavorecido
                });

                insertPessoas([newPessoa]);
              }

              const saque = new Saque({
                favorecido: newPessoa._id || pessoa._id,
                municipio: cidade._id,
                mesCompetencia,
                mesCompetencia,
                data,
                valor: parseFloat(valor)
              });
              return saque;
            })
        )
      )
        .then(saques => {
          const saquesNormalizados = saques.filter(saque => saque);
          insertSaques(saquesNormalizados);
        })
        .catch(err => {
          console.log("Erro", err);
          process.exit(0);
        });
    }
  },

  Cidade: require("./cidade.model"),
  Pessoa: require("./pessoa.model"),
  Saque: require("./saque.model")
};

function insertSaques(saques) {
  var end = Date.now();
  var timeSpent = (end - begin) / 1000 + "secs";
  saquesCriados += saques.length;

  console.log(
    `Saques criados: ${saquesCriados} - Pessoas criadas: ${pessoasCriadas} - Cidades criadas: ${cidadesCriadas} - Tempo gasto ${timeSpent}`
  );

  Saque.insertMany(saques);
}

function insertCidades(cidades) {
  if (cidades.length > 0) {
    cidadesCriadas += cidades.length;
    Cidade.insertMany(cidades);
  }
}

function insertPessoas(pessoas) {
  if (pessoas.length > 0) {
    Pessoa.insertMany(pessoas);
    pessoasCriadas += pessoas.length;
  }
}

function getCidade(cidade) {
  return Cidade.findOne(cidade);
}

function getPessoa(pessoa) {
  return Pessoa.findOne(pessoa).then(pessoa => {
    if (!pessoa) {
      return {};
    } else return pessoa;
  });
}
