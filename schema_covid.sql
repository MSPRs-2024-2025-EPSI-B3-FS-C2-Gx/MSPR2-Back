-- Création de la table avec la structure adaptée
CREATE TABLE covid19_data (
    id SERIAL PRIMARY KEY,  -- Clé primaire auto-incrémentée

    -- Données journalières
    country VARCHAR(100) NOT NULL,  -- Nom du pays
    date TIMESTAMP NOT NULL,  -- Date avec heure

    cumulative_total_cases FLOAT,  -- Total cumulé de cas
    daily_new_cases FLOAT,  -- Nouveaux cas quotidiens
    daily_active_cases FLOAT,  -- Cas actifs quotidiens
    cumulative_total_deaths FLOAT,  -- Total cumulé de décès
    daily_new_deaths FLOAT,  -- Nouveaux décès quotidiens
    daily_mortality_rate FLOAT,  -- Taux de mortalité quotidien (%)
    daily_recovery_rate FLOAT,  -- Taux de récupération quotidien (%)

    -- Données résumées
    continent VARCHAR(50),  -- Continent
    total_confirmed INTEGER,  -- Total des cas confirmés
    total_deaths FLOAT,  -- Total des décès
    total_recovered FLOAT,  -- Total des récupérations
    summary_active_cases FLOAT,  -- Cas actifs résumés
    serious_or_critical FLOAT,  -- Cas critiques ou graves
    total_cases_per_1m_population FLOAT,  -- Cas par million d'habitants
    total_deaths_per_1m_population FLOAT,  -- Décès par million d'habitants
    total_tests FLOAT,  -- Total des tests effectués
    total_tests_per_1m_population FLOAT,  -- Tests par million d'habitants
    population BIGINT,  -- Population totale
    summary_mortality_rate FLOAT,  -- Taux de mortalité résumé (%)
    summary_recovery_rate FLOAT  -- Taux de récupération résumé (%)
);