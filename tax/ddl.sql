-- Federal + state income-tax filing model (Databricks / Unity Catalog, Delta).
-- Catalog/schema: change exalabs.tax if needed (search & replace).
-- Run top to bottom in Databricks SQL. Dimensions first so the informational
-- FK constraints on the facts resolve.

CREATE SCHEMA IF NOT EXISTS exalabs.tax
COMMENT 'Federal and state tax filing demo: 2 large facts + shared dimensions';

-- ---------------------------------------------------------------- dimensions
CREATE OR REPLACE TABLE exalabs.tax.dim_tax_year (
  tax_year         INT        NOT NULL COMMENT 'Tax year, e.g. 2025',
  filing_deadline  DATE,
  is_current_year  BOOLEAN,
  CONSTRAINT pk_dim_tax_year PRIMARY KEY (tax_year) RELY
) USING DELTA COMMENT '~10 rows';

CREATE OR REPLACE TABLE exalabs.tax.dim_entity (
  entity_key    INT    NOT NULL,
  entity_code   STRING NOT NULL,
  entity_name   STRING NOT NULL COMMENT 'Employing entity / business unit',
  region        STRING,
  entity_type   STRING,
  CONSTRAINT pk_dim_entity PRIMARY KEY (entity_key) RELY
) USING DELTA COMMENT '~200 rows. Row-security boundary (Sigma user attribute: entity).';

CREATE OR REPLACE TABLE exalabs.tax.dim_state (
  state_key         INT    NOT NULL,
  state_code        STRING NOT NULL,
  state_name        STRING NOT NULL,
  is_territory      BOOLEAN,
  has_income_tax    BOOLEAN,
  CONSTRAINT pk_dim_state PRIMARY KEY (state_key) RELY
) USING DELTA COMMENT '55 rows: 50 states, DC, PR, GU, VI, AS';

CREATE OR REPLACE TABLE exalabs.tax.dim_income_type (
  income_type_key   INT    NOT NULL,
  income_type_name  STRING NOT NULL,
  income_category   STRING COMMENT 'Earned / Investment / Equity / Other',
  CONSTRAINT pk_dim_income_type PRIMARY KEY (income_type_key) RELY
) USING DELTA COMMENT '~40 rows: wages, bonus, equity, ...';

CREATE OR REPLACE TABLE exalabs.tax.dim_tax_line (
  tax_line_key      INT    NOT NULL,
  tax_line_code     STRING NOT NULL,
  tax_line_name     STRING NOT NULL,
  jurisdiction      STRING NOT NULL COMMENT 'FEDERAL | STATE',
  line_category     STRING COMMENT 'INCOME | ADJUSTMENT | DEDUCTION | TAXABLE | TAX | CREDIT | PAYMENT',
  income_type_key   INT    COMMENT 'Set for INCOME lines only',
  line_sequence     INT    COMMENT 'Order of the calculation trail',
  is_core_line      BOOLEAN,
  CONSTRAINT pk_dim_tax_line PRIMARY KEY (tax_line_key) RELY
) USING DELTA COMMENT '~300 rows: calculation line definitions';

CREATE OR REPLACE TABLE exalabs.tax.dim_employee (
  employee_key     BIGINT NOT NULL COMMENT 'Surrogate key (one row per version)',
  employee_id      STRING NOT NULL COMMENT 'Business key',
  employee_name    STRING,
  entity_key       INT,
  home_state_key   INT,
  filing_status    STRING,
  effective_from   DATE,
  effective_to     DATE,
  is_current       BOOLEAN,
  CONSTRAINT pk_dim_employee PRIMARY KEY (employee_key) RELY
) USING DELTA CLUSTER BY (entity_key) COMMENT '500K-1M rows incl. history';

-- ------------------------------------------------------------- return status
-- One row per employee x tax year. Answers "complete / in review / amended"
-- without COUNT(DISTINCT) over 200M detail rows.
CREATE OR REPLACE TABLE exalabs.tax.fact_return (
  return_id       STRING NOT NULL,
  employee_key    BIGINT NOT NULL,
  entity_key      INT    NOT NULL,
  tax_year        INT    NOT NULL,
  return_status   STRING NOT NULL COMMENT 'COMPLETE | IN_REVIEW | AMENDED',
  filed_date      DATE,
  load_ts         TIMESTAMP,
  CONSTRAINT pk_fact_return PRIMARY KEY (return_id) RELY
) USING DELTA CLUSTER BY (tax_year, entity_key) COMMENT '~2.5M-5M rows';

-- --------------------------------------------------------------------- facts
CREATE OR REPLACE TABLE exalabs.tax.fact_federal_tax_detail (
  return_id        STRING NOT NULL,
  employee_key     BIGINT NOT NULL,
  entity_key       INT    NOT NULL,
  tax_year         INT    NOT NULL,
  tax_line_key     INT    NOT NULL,
  income_type_key  INT    COMMENT 'Populated on INCOME lines; drives Federal summary',
  amount           DECIMAL(18,2),
  load_ts          TIMESTAMP,
  CONSTRAINT fk_fed_entity FOREIGN KEY (entity_key)    REFERENCES exalabs.tax.dim_entity(entity_key),
  CONSTRAINT fk_fed_year   FOREIGN KEY (tax_year)      REFERENCES exalabs.tax.dim_tax_year(tax_year),
  CONSTRAINT fk_fed_line   FOREIGN KEY (tax_line_key)  REFERENCES exalabs.tax.dim_tax_line(tax_line_key),
  CONSTRAINT fk_fed_emp    FOREIGN KEY (employee_key)  REFERENCES exalabs.tax.dim_employee(employee_key)
) USING DELTA
CLUSTER BY (tax_year, entity_key, employee_key)   -- liquid clustering: data skipping on the demo's filters
TBLPROPERTIES ('delta.enableDeletionVectors'='true','delta.autoOptimize.autoCompact'='true')
COMMENT 'Grain: employee x federal calculation line x tax year. 50-100M rows';

CREATE OR REPLACE TABLE exalabs.tax.fact_state_tax_detail (
  return_id        STRING NOT NULL,
  employee_key     BIGINT NOT NULL,
  entity_key       INT    NOT NULL,
  state_key        INT    NOT NULL,
  tax_year         INT    NOT NULL,
  tax_line_key     INT    NOT NULL,
  amount           DECIMAL(18,2),
  federal_agi_ref  DECIMAL(18,2) COMMENT 'Federal AGI this state calc started from',
  load_ts          TIMESTAMP,
  CONSTRAINT fk_st_entity FOREIGN KEY (entity_key)   REFERENCES exalabs.tax.dim_entity(entity_key),
  CONSTRAINT fk_st_state  FOREIGN KEY (state_key)    REFERENCES exalabs.tax.dim_state(state_key),
  CONSTRAINT fk_st_year   FOREIGN KEY (tax_year)     REFERENCES exalabs.tax.dim_tax_year(tax_year),
  CONSTRAINT fk_st_line   FOREIGN KEY (tax_line_key) REFERENCES exalabs.tax.dim_tax_line(tax_line_key),
  CONSTRAINT fk_st_emp    FOREIGN KEY (employee_key) REFERENCES exalabs.tax.dim_employee(employee_key)
) USING DELTA
CLUSTER BY (tax_year, entity_key, state_key, employee_key)
TBLPROPERTIES ('delta.enableDeletionVectors'='true','delta.autoOptimize.autoCompact'='true')
COMMENT 'Grain: employee x state x state calculation line x tax year. 75-150M rows';

-- ------------------------------------------------- optional gold views (demo 5)
-- "Logic in Databricks" counterpart to the Sigma-materialized summaries.
CREATE OR REPLACE VIEW exalabs.tax.v_federal_summary AS
SELECT f.entity_key, f.tax_year, f.income_type_key,
       SUM(CASE WHEN l.line_category = 'INCOME' THEN f.amount END) AS gross_income,
       SUM(CASE WHEN l.tax_line_code = 'F_AGI' THEN f.amount END)  AS federal_agi,
       SUM(CASE WHEN l.tax_line_code = 'F_TAXABLE' THEN f.amount END) AS federal_taxable_income,
       SUM(CASE WHEN l.tax_line_code = 'F_TAX' THEN f.amount END)  AS federal_tax
FROM exalabs.tax.fact_federal_tax_detail f
JOIN exalabs.tax.dim_tax_line l USING (tax_line_key)
GROUP BY f.entity_key, f.tax_year, f.income_type_key;
-- Note: AGI/taxable/tax lines carry income_type_key NULL, so the Sigma summary
-- (entity x year x income type) takes income from this grain and tax lines at
-- entity x year; the effective-rate element joins the two at entity x year.

CREATE OR REPLACE VIEW exalabs.tax.v_state_summary AS
SELECT s.entity_key, s.state_key, s.tax_year,
       SUM(CASE WHEN l.tax_line_code = 'S_TAX' THEN s.amount END)     AS state_tax,
       SUM(CASE WHEN l.tax_line_code = 'S_TAXABLE' THEN s.amount END) AS state_taxable_income
FROM exalabs.tax.fact_state_tax_detail s
JOIN exalabs.tax.dim_tax_line l USING (tax_line_key)
GROUP BY s.entity_key, s.state_key, s.tax_year;
