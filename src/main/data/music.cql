CREATE KEYSPACE IF NOT EXISTS spark_demo
WITH replication = {
  'class': 'SimpleStrategy', 
  'replication_factor' : 1
};

USE spark_demo;

CREATE TABLE IF NOT EXISTS countries(name TEXT PRIMARY KEY);

CREATE TABLE IF NOT EXISTS performers (
  name TEXT,
  country TEXT,
  gender TEXT,
  type TEXT,
  born TEXT,
  died TEXT,
  styles LIST<TEXT>,
  PRIMARY KEY (name)
);

CREATE TABLE IF NOT EXISTS albums (
  performer TEXT,
  title TEXT,
  year INT,
  country TEXT,
  quality TEXT,
  status TEXT,
  PRIMARY KEY(title)
);

CREATE TABLE IF NOT EXISTS performers_by_style (
  style TEXT,
  performer TEXT,
  PRIMARY KEY (style, performer)
);

CREATE TABLE IF NOT EXISTS performers_distribution_by_style (
  type text, 
  style text,
  count int,
  PRIMARY KEY (type, style)
);

CREATE TABLE IF NOT EXISTS top10_styles (
  type text, 
  count int,
  style text,
  PRIMARY KEY (type, count)
) WITH CLUSTERING ORDER BY (count DESC);

CREATE TABLE IF NOT EXISTS albums_by_decade_and_country(  
  country text,
  decade text,
  album_count int,
  PRIMARY KEY(decade,country)
);

CREATE TABLE IF NOT EXISTS albums_by_decade_and_country_sql(
    country text,
    decade text,
    album_count int,
    PRIMARY KEY(decade,country)
  );
        
TRUNCATE performers;
TRUNCATE albums;

COPY performers(name, country, gender, type, born, died,styles)
FROM 'performers.csv';

COPY albums(performer,title, year, country, quality, status)
FROM 'albums.csv';

COPY countries(name) FROM 'countries.csv';
