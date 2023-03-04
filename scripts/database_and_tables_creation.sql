-- Creating SQL DataBase and Table

CREATE DATABASE inovacao;

\c inovacao

CREATE TABLE projeto (
   edital VARCHAR(150),
   instituicao_fomento VARCHAR(200),
   tipo_projeto VARCHAR(200),
   valor_total_bolsas NUMERIC(12,2),
   valor_total_auxilio NUMERIC(12,2)
   );


