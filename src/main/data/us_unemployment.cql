CREATE TABLE IF NOT EXISTS spark_demo.us_unemployment_stats (
    year int PRIMARY KEY,
    agriculture_part_count int,
    civil_labor_count int,
    civil_non_institutional_count int,
    employed_count int,
    employed_percentage double,
    footnotes text,
    labor_population_percentage double,
    non_agriculture_part_count int,
    not_labor_count int,
    unemployed_count int,
    unemployed_percentage_to_labor double
);
