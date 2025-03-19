CREATE TABLE country_statistics (
    Country VARCHAR(255),
    total_cases INT NOT NULL,
    total_vaccinated INT NOT NULL,
    population INT NOT NULL,
    PRIMARY KEY (Country)
);