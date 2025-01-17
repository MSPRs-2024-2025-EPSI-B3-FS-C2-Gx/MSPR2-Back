CREATE TABLE IF NOT EXISTS covid_global_yearly_summary (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL CHECK (year >= 2020 AND year <= 2024),
    total_new_cases BIGINT CHECK (total_new_cases >= 0),
    total_new_deaths BIGINT CHECK (total_new_deaths >= 0),
    total_cumulative_cases BIGINT CHECK (total_cumulative_cases >= 0),
    total_cumulative_deaths BIGINT CHECK (total_cumulative_deaths >= 0),
    cfr NUMERIC(5, 2) CHECK (cfr >= 0)  -- Taux de létalité (CFR) en pourcentage
);

-- Création ou modification de la table covid_region_yearly_summary avec la colonne Year_Date
CREATE TABLE IF NOT EXISTS covid_region_yearly_summary (
    id SERIAL PRIMARY KEY,
    WHO_region VARCHAR(50),
    Year INTEGER,
    total_new_cases BIGINT CHECK (total_new_cases >= 0),
    total_new_deaths BIGINT CHECK (total_new_deaths >= 0),
    Year_Date TIMESTAMP
);

-- Mise à jour de la colonne Year_Date pour correspondre à l'année
UPDATE covid_region_yearly_summary
SET Year_Date = TO_TIMESTAMP(Year || '-01-01', 'YYYY-MM-DD');