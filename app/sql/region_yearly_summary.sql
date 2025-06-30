CREATE TABLE region_yearly_summary (
  WHO_region VARCHAR(50) NOT NULL,
  Year INT NOT NULL,
  total_cases INT NOT NULL,
  total_deaths INT NOT NULL,
  PRIMARY KEY (WHO_region, Year)
);
