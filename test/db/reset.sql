DROP TABLE IF EXISTS people CASCADE;
DROP TABLE IF EXISTS posts CASCADE;

CREATE TABLE people (
  id serial PRIMARY KEY,
  name varchar(30),
  age integer,
  bio text
);

CREATE TABLE posts (
  id serial PRIMARY KEY,
  person_id integer REFERENCES people(id),
  topic varchar(30) NOT NULL,
  body text
);
