CREATE TABLE IF NOT EXISTS covid_region_summary (
    id SERIAL PRIMARY KEY,
    who_region VARCHAR(20),
    vaccine_period VARCHAR(20),  -- "Before Vaccine" ou "After Vaccine"
    total_cumulative_cases NUMERIC(20, 0) CHECK (total_cumulative_cases >= 0),
    total_new_cases NUMERIC(20, 0) CHECK (total_new_cases >= 0),
    total_new_deaths NUMERIC(20, 0) CHECK (total_new_deaths >= 0)
);