/*
Schema for storing raw ISD-lite hourly weather data.
More can be read about these weather sets here: http://www.ncdc.noaa.gov/oa/climate/isd/
*/

CREATE KEYSPACE IF NOT EXISTS spark_demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE spark_demo;

/*
Weather stations that correspond with collected raw weather data.
You can load this data by using the stations.csv file and the following copy command:

COPY weather_station (id, name, country_code, state_code, call_sign, lat, long, elevation) FROM 'stations.csv';

*/

CREATE TABLE IF NOT EXISTS weather_station (
    id text PRIMARY KEY,  // Composite of Air Force Datsav3 station number and NCDC WBAN number
    name text,            // Name of reporting station
    country_code text,    // 2 letter ISO Country ID
    state_code text,           // 2 letter state code for US stations
    call_sign text,       // International station call sign
    lat float,            // Latitude in decimal degrees
    long float,           // Longitude in decimal degrees
    elevation float       // Elevation in meters
);

TRUNCATE weather_station;
//CREATE INDEX weather_station_country ON weather_station (country_code);
COPY weather_station (id, name, country_code, state_code, call_sign, lat, long, elevation) FROM 'stations.csv';

/*
Raw weather readings from a single station, hourly.

You can try this COPY command but it's a lot of data to try to load. You'll be better off with some other loading program.
COPY raw_weather_data (weather_station, year, month, day, hour, temperature, dewpoint, pressure, wind_direction, wind_speed, sky_condition, one_hour_precip, six_hour_precip)
FROM '2005.csv';

*/

/* 010010:99999, //composite partition key
   2014,01,01,01, // YYYY MM DD hh
   -0.4, //temperature in Celsius
   -4.0, //dew point in Celsius
   1012.1, // sea level pressure in Hecto Pascal
   360.0, // wind direction in ° (0-360)
   4.0, // wind speed in m/s
   0.0, // sky condition coded
   0.0, // one-hour accumulated liquid precipitation in mm
   0.0 // six-hour accumulated liquid precipitation in mm
*/

CREATE TABLE IF NOT EXISTS raw_weather_data (
   weather_station text,    // Composite of Air Force Datsav3 station number and NCDC WBAN number
   year int,                // Year collected
   month int,               // Month collected
   day int,                 // Day collected
   hour int,                // Hour collected
   temperature float,       // Air temperature (degrees Celsius)
   dewpoint float,          // Dew point temperature (degrees Celsius)
   pressure float,          // Sea level pressure (hectopascals)
   wind_direction int,      // Wind direction in degrees. 0-359
   wind_speed float,        // Wind speed (meters per second)
   sky_condition int,       // Total cloud cover (coded, see format documentation)
   sky_condition_text text, // Non-coded sky conditions
   one_hour_precip float,   // One-hour accumulated liquid precipitation (millimeters)
   six_hour_precip float,   // Six-hour accumulated liquid precipitation (millimeters)
   PRIMARY KEY ((weather_station), year, month, day, hour)
) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC);

TRUNCATE raw_weather_data;

/*
Quick access lookup table for sky_condition. Useful for potential analytics.

Just in case you want to know what an okta is, here is the wikipedia page:
http://en.wikipedia.org/wiki/Okta
*/

CREATE TABLE IF NOT EXISTS sky_condition_lookup (
    code int PRIMARY KEY,
    condition text
);

TRUNCATE sky_condition_lookup;

INSERT INTO  sky_condition_lookup (code, condition) VALUES (0, 'None, SKC or CLR');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (1, 'One okta - 1/10 or less but not zero');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (2, 'Two oktas - 2/10 - 3/10, or FEW');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (3, 'Three oktas - 4/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (4, 'Four oktas - 5/10, or SCT');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (5, 'Five oktas - 6/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (6, 'Six oktas - 7/10 - 8/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (7, 'Seven oktas - 9/10 or more but not 10/10, or BKN');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (8, 'Eight oktas - 10/10, or OVC');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (9, 'Sky obscured, or cloud amount cannot be estimated');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (10, 'Partial obscuration 11: Thin scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (12, 'Scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (13, 'Dark scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (14, 'Thin broken 15: Broken');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (16, 'Dark broken 17: Thin overcast 18: Overcast');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (19, 'Dark overcast');
