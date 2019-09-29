"use strict";
const fs = require("fs");
const csv = require("fast-csv");

fs.createReadStream("./teste.csv", {
encoding: "utf8"
})
    //.pipe(csv.parse({ delimiter: ";" }))
    .on("data", function(data) {
    console.log(data)
    })
    .on("end", function(data) {
    console.log("________________________________________________");
    console.log("Arquivo importado com sucesso");
    }) 
    .on("error", function(err) {
    console.log(err);
    process.exit(0);
    });

