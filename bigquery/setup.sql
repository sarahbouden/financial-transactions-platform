-- Crée le dataset principal
CREATE SCHEMA IF NOT EXISTS `your-project-id.fintech_dw`
OPTIONS (
  location = 'EU'
);

-- Crée les datasets par couche dbt
CREATE SCHEMA IF NOT EXISTS `your-project-id.fintech_dw_staging`
OPTIONS (location = 'EU');

CREATE SCHEMA IF NOT EXISTS `your-project-id.fintech_dw_intermediate`
OPTIONS (location = 'EU');

CREATE SCHEMA IF NOT EXISTS `your-project-id.fintech_dw_marts`
OPTIONS (location = 'EU');

-- Table RAW (ingestion Pub/Sub via BigQuery Subscription)
CREATE TABLE IF NOT EXISTS `your-project-id.fintech_dw.raw_transactions` (
  data          JSON,
  subscription_name STRING,
  message_id    STRING,
  publish_time  TIMESTAMP,
  attributes    JSON
);