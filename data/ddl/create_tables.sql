-- Legacy source schema for the synthetic insurer (SQL Server 2022).
-- Loaded by: python -m data.load_mssql
IF DB_ID('InsuranceLegacy') IS NULL CREATE DATABASE InsuranceLegacy;
GO
USE InsuranceLegacy;
GO
IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');   -- staging tables written by legacy jobs
GO
IF SCHEMA_ID('rpt') IS NULL EXEC('CREATE SCHEMA rpt');   -- report tables written by legacy jobs
GO
DROP TABLE IF EXISTS dbo.claim_payments, dbo.claims, dbo.premiums, dbo.policies,
                     dbo.customers, dbo.brokers, dbo.products;
GO
CREATE TABLE dbo.products (
    product_code        VARCHAR(10)   NOT NULL,
    product_name        VARCHAR(60)   NOT NULL,
    line_of_business    VARCHAR(10)   NOT NULL,
    base_annual_premium DECIMAL(18,2) NOT NULL
);
CREATE TABLE dbo.brokers (
    broker_id       VARCHAR(10)  NOT NULL,
    broker_name     VARCHAR(120) NOT NULL,
    county          VARCHAR(30)  NULL,
    commission_rate DECIMAL(6,4) NULL,
    active_flag     CHAR(1)      NULL
);
CREATE TABLE dbo.customers (
    customer_id    VARCHAR(10) NOT NULL,
    first_name     VARCHAR(60) NULL,
    last_name      VARCHAR(60) NULL,
    date_of_birth  DATE        NULL,
    county         VARCHAR(30) NULL,
    customer_since DATE        NULL,
    segment        VARCHAR(10) NULL
);
CREATE TABLE dbo.policies (
    policy_id         VARCHAR(12)   NOT NULL,
    customer_id       VARCHAR(10)   NOT NULL,
    product_code      VARCHAR(10)   NOT NULL,
    broker_id         VARCHAR(10)   NULL,
    channel           VARCHAR(10)   NULL,
    start_date        DATE          NOT NULL,
    end_date          DATE          NULL,
    status            VARCHAR(20)   NULL,
    annual_premium    DECIMAL(18,2) NULL,
    payment_frequency VARCHAR(10)   NULL,
    sum_insured       DECIMAL(18,2) NULL
);
CREATE TABLE dbo.premiums (
    premium_id VARCHAR(12)   NOT NULL,   -- not unique: duplicate rows are deliberate
    policy_id  VARCHAR(12)   NOT NULL,
    due_date   DATE          NOT NULL,
    paid_date  DATE          NULL,
    amount     DECIMAL(18,3) NULL         -- 3 dp: half-cent values are deliberate
);
CREATE TABLE dbo.claims (
    claim_id       VARCHAR(12)   NOT NULL,
    policy_id      VARCHAR(12)   NOT NULL,
    loss_date      DATE          NULL,
    reported_date  DATE          NULL,
    claim_type     VARCHAR(20)   NULL,
    status         VARCHAR(20)   NULL,
    reserve_amount DECIMAL(18,2) NULL
);
CREATE TABLE dbo.claim_payments (
    payment_id   VARCHAR(12)   NOT NULL,  -- not unique: duplicate rows are deliberate
    claim_id     VARCHAR(12)   NOT NULL,
    payment_date DATE          NULL,
    payment_type VARCHAR(12)   NULL,
    amount       DECIMAL(18,3) NULL
);
GO
