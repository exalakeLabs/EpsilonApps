-- Databricks SQL schema inspired by the TPC-DS decision-support model.
-- Creates managed Delta tables in the tpchds schema. This is not an official TPC kit.
-- Primary/foreign key and uniqueness rules are intentionally not declared:
-- Delta Lake does not enforce them, and analytical loads commonly arrive in bulk.

CREATE SCHEMA IF NOT EXISTS tpchds;
USE SCHEMA tpchds;

CREATE TABLE IF NOT EXISTS date_dim (
    d_date_sk              BIGINT,
    d_date_id              STRING NOT NULL,
    d_date                 DATE NOT NULL,
    d_month_seq            INT,
    d_week_seq             INT,
    d_quarter_seq          INT,
    d_year                 INT NOT NULL,
    d_dow                  SMALLINT,
    d_moy                  SMALLINT,
    d_dom                  SMALLINT,
    d_qoy                  SMALLINT,
    d_fy_year              INT,
    d_fy_quarter_seq       INT,
    d_fy_week_seq          INT,
    d_day_name             STRING,
    d_quarter_name         STRING,
    d_holiday              BOOLEAN,
    d_weekend              BOOLEAN,
    d_following_holiday    BOOLEAN,
    d_first_dom            INT,
    d_last_dom             INT,
    d_same_day_ly          INT,
    d_same_day_lq          INT,
    d_current_day          BOOLEAN,
    d_current_week         BOOLEAN,
    d_current_month        BOOLEAN,
    d_current_quarter      BOOLEAN,
    d_current_year         BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS time_dim (
    t_time_sk              BIGINT,
    t_time_id              STRING NOT NULL,
    t_time                 INT NOT NULL,
    t_hour                 SMALLINT NOT NULL,
    t_minute               SMALLINT NOT NULL,
    t_second               SMALLINT NOT NULL,
    t_am_pm                STRING,
    t_shift                STRING,
    t_sub_shift            STRING,
    t_meal_time            STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS income_band (
    ib_income_band_sk      BIGINT,
    ib_lower_bound         INT NOT NULL,
    ib_upper_bound         INT NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS reason (
    r_reason_sk            BIGINT,
    r_reason_id            STRING NOT NULL,
    r_reason_desc          STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS ship_mode (
    sm_ship_mode_sk        BIGINT,
    sm_ship_mode_id        STRING NOT NULL,
    sm_type                STRING,
    sm_code                STRING,
    sm_carrier             STRING,
    sm_contract            STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS warehouse (
    w_warehouse_sk         BIGINT,
    w_warehouse_id         STRING NOT NULL,
    w_warehouse_name       STRING,
    w_warehouse_sq_ft      INT,
    w_street_number        STRING,
    w_street_name          STRING,
    w_street_type          STRING,
    w_suite_number         STRING,
    w_city                 STRING,
    w_county               STRING,
    w_state                STRING,
    w_zip                  STRING,
    w_country              STRING,
    w_gmt_offset           DECIMAL(5,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS customer_address (
    ca_address_sk          BIGINT,
    ca_address_id          STRING NOT NULL,
    ca_street_number       STRING,
    ca_street_name         STRING,
    ca_street_type         STRING,
    ca_suite_number        STRING,
    ca_city                STRING,
    ca_county              STRING,
    ca_state               STRING,
    ca_zip                 STRING,
    ca_country             STRING,
    ca_gmt_offset          DECIMAL(5,2),
    ca_location_type       STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS customer_demographics (
    cd_demo_sk             BIGINT,
    cd_gender              STRING,
    cd_marital_status      STRING,
    cd_education_status    STRING,
    cd_purchase_estimate   INT,
    cd_credit_rating       STRING,
    cd_dep_count           SMALLINT,
    cd_dep_employed_count  SMALLINT,
    cd_dep_college_count   SMALLINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS household_demographics (
    hd_demo_sk             BIGINT,
    hd_income_band_sk      BIGINT,
    hd_buy_potential       STRING,
    hd_dep_count           SMALLINT,
    hd_vehicle_count       SMALLINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS customer (
    c_customer_sk          BIGINT,
    c_customer_id          STRING NOT NULL,
    c_current_cdemo_sk     BIGINT,
    c_current_hdemo_sk     BIGINT,
    c_current_addr_sk      BIGINT,
    c_first_shipto_date_sk BIGINT,
    c_first_sales_date_sk  BIGINT,
    c_salutation           STRING,
    c_first_name           STRING,
    c_last_name            STRING,
    c_preferred_cust_flag  BOOLEAN,
    c_birth_day            SMALLINT,
    c_birth_month          SMALLINT,
    c_birth_year           INT,
    c_birth_country        STRING,
    c_login                STRING,
    c_email_address        STRING,
    c_last_review_date_sk  BIGINT
) USING DELTA;

CREATE TABLE IF NOT EXISTS item (
    i_item_sk              BIGINT,
    i_item_id              STRING NOT NULL,
    i_rec_start_date       DATE,
    i_rec_end_date         DATE,
    i_item_desc            STRING,
    i_current_price        DECIMAL(15,2),
    i_wholesale_cost       DECIMAL(15,2),
    i_brand_id             INT,
    i_brand                STRING,
    i_class_id             INT,
    i_class                STRING,
    i_category_id          INT,
    i_category             STRING,
    i_manufact_id          INT,
    i_manufact             STRING,
    i_size                 STRING,
    i_formulation          STRING,
    i_color                STRING,
    i_units                STRING,
    i_container            STRING,
    i_manager_id           INT,
    i_product_name         STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS promotion (
    p_promo_sk             BIGINT,
    p_promo_id             STRING NOT NULL,
    p_start_date_sk        BIGINT,
    p_end_date_sk          BIGINT,
    p_item_sk              BIGINT,
    p_cost                 DECIMAL(15,2),
    p_response_target      INT,
    p_promo_name           STRING,
    p_channel_dmail        BOOLEAN,
    p_channel_email        BOOLEAN,
    p_channel_catalog      BOOLEAN,
    p_channel_tv           BOOLEAN,
    p_channel_radio        BOOLEAN,
    p_channel_press        BOOLEAN,
    p_channel_event        BOOLEAN,
    p_channel_demo         BOOLEAN,
    p_channel_details      STRING,
    p_purpose              STRING,
    p_discount_active      BOOLEAN
) USING DELTA;

CREATE TABLE IF NOT EXISTS store (
    s_store_sk             BIGINT,
    s_store_id             STRING NOT NULL,
    s_rec_start_date       DATE,
    s_rec_end_date         DATE,
    s_closed_date_sk       BIGINT,
    s_store_name           STRING,
    s_number_employees     INT,
    s_floor_space          INT,
    s_hours                STRING,
    s_manager              STRING,
    s_market_id            INT,
    s_geography_class      STRING,
    s_market_desc          STRING,
    s_market_manager       STRING,
    s_division_id          INT,
    s_division_name        STRING,
    s_company_id           INT,
    s_company_name         STRING,
    s_street_number        STRING,
    s_street_name          STRING,
    s_street_type          STRING,
    s_suite_number         STRING,
    s_city                 STRING,
    s_county               STRING,
    s_state                STRING,
    s_zip                  STRING,
    s_country              STRING,
    s_gmt_offset           DECIMAL(5,2),
    s_tax_percentage       DECIMAL(5,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS call_center (
    cc_call_center_sk      BIGINT,
    cc_call_center_id      STRING NOT NULL,
    cc_rec_start_date      DATE,
    cc_rec_end_date        DATE,
    cc_closed_date_sk      BIGINT,
    cc_open_date_sk        BIGINT,
    cc_name                STRING,
    cc_class               STRING,
    cc_employees           INT,
    cc_sq_ft               INT,
    cc_hours               STRING,
    cc_manager             STRING,
    cc_market_id           INT,
    cc_market_class        STRING,
    cc_market_desc         STRING,
    cc_market_manager      STRING,
    cc_division            INT,
    cc_division_name       STRING,
    cc_company             INT,
    cc_company_name        STRING,
    cc_street_number       STRING,
    cc_street_name         STRING,
    cc_street_type         STRING,
    cc_suite_number        STRING,
    cc_city                STRING,
    cc_county              STRING,
    cc_state               STRING,
    cc_zip                 STRING,
    cc_country             STRING,
    cc_gmt_offset          DECIMAL(5,2),
    cc_tax_percentage      DECIMAL(5,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_page (
    cp_catalog_page_sk     BIGINT,
    cp_catalog_page_id     STRING NOT NULL,
    cp_start_date_sk       BIGINT,
    cp_end_date_sk         BIGINT,
    cp_department          STRING,
    cp_catalog_number      INT,
    cp_catalog_page_number INT,
    cp_description         STRING,
    cp_type                STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS web_site (
    web_site_sk            BIGINT,
    web_site_id            STRING NOT NULL,
    web_rec_start_date     DATE,
    web_rec_end_date       DATE,
    web_name               STRING,
    web_open_date_sk       BIGINT,
    web_close_date_sk      BIGINT,
    web_class              STRING,
    web_manager            STRING,
    web_market_id          INT,
    web_market_class       STRING,
    web_market_desc        STRING,
    web_market_manager     STRING,
    web_company_id         INT,
    web_company_name       STRING,
    web_street_number      STRING,
    web_street_name        STRING,
    web_street_type        STRING,
    web_suite_number       STRING,
    web_city               STRING,
    web_county             STRING,
    web_state              STRING,
    web_zip                STRING,
    web_country            STRING,
    web_gmt_offset         DECIMAL(5,2),
    web_tax_percentage     DECIMAL(5,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS web_page (
    wp_web_page_sk         BIGINT,
    wp_web_page_id         STRING NOT NULL,
    wp_rec_start_date      DATE,
    wp_rec_end_date        DATE,
    wp_creation_date_sk    BIGINT,
    wp_access_date_sk      BIGINT,
    wp_autogen_flag        BOOLEAN,
    wp_customer_sk         BIGINT,
    wp_url                 STRING,
    wp_type                STRING,
    wp_char_count          INT,
    wp_link_count          INT,
    wp_image_count         INT,
    wp_max_ad_count        INT
) USING DELTA;

CREATE TABLE IF NOT EXISTS inventory (
    inv_date_sk            BIGINT NOT NULL,
    inv_item_sk            BIGINT NOT NULL,
    inv_warehouse_sk       BIGINT NOT NULL,
    inv_quantity_on_hand   INT NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS store_sales (
    ss_sold_date_sk        BIGINT,
    ss_sold_time_sk        BIGINT,
    ss_item_sk             BIGINT NOT NULL,
    ss_customer_sk         BIGINT,
    ss_cdemo_sk            BIGINT,
    ss_hdemo_sk            BIGINT,
    ss_addr_sk             BIGINT,
    ss_store_sk            BIGINT,
    ss_promo_sk            BIGINT,
    ss_ticket_number       BIGINT NOT NULL,
    ss_quantity            INT,
    ss_wholesale_cost      DECIMAL(15,2),
    ss_list_price          DECIMAL(15,2),
    ss_sales_price         DECIMAL(15,2),
    ss_ext_discount_amt    DECIMAL(15,2),
    ss_ext_sales_price     DECIMAL(15,2),
    ss_ext_wholesale_cost  DECIMAL(15,2),
    ss_ext_list_price      DECIMAL(15,2),
    ss_ext_tax             DECIMAL(15,2),
    ss_coupon_amt          DECIMAL(15,2),
    ss_net_paid            DECIMAL(15,2),
    ss_net_paid_inc_tax    DECIMAL(15,2),
    ss_net_profit          DECIMAL(15,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS store_returns (
    sr_returned_date_sk    BIGINT,
    sr_return_time_sk      BIGINT,
    sr_item_sk             BIGINT NOT NULL,
    sr_customer_sk         BIGINT,
    sr_cdemo_sk            BIGINT,
    sr_hdemo_sk            BIGINT,
    sr_addr_sk             BIGINT,
    sr_store_sk            BIGINT,
    sr_reason_sk           BIGINT,
    sr_ticket_number       BIGINT NOT NULL,
    sr_return_quantity     INT,
    sr_return_amt          DECIMAL(15,2),
    sr_return_tax          DECIMAL(15,2),
    sr_return_amt_inc_tax  DECIMAL(15,2),
    sr_fee                 DECIMAL(15,2),
    sr_return_ship_cost    DECIMAL(15,2),
    sr_refunded_cash       DECIMAL(15,2),
    sr_reversed_charge     DECIMAL(15,2),
    sr_store_credit        DECIMAL(15,2),
    sr_net_loss            DECIMAL(15,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_sales (
    cs_sold_date_sk        BIGINT,
    cs_sold_time_sk        BIGINT,
    cs_ship_date_sk        BIGINT,
    cs_bill_customer_sk    BIGINT,
    cs_bill_cdemo_sk       BIGINT,
    cs_bill_hdemo_sk       BIGINT,
    cs_bill_addr_sk        BIGINT,
    cs_ship_customer_sk    BIGINT,
    cs_ship_cdemo_sk       BIGINT,
    cs_ship_hdemo_sk       BIGINT,
    cs_ship_addr_sk        BIGINT,
    cs_call_center_sk      BIGINT,
    cs_catalog_page_sk     BIGINT,
    cs_ship_mode_sk        BIGINT,
    cs_warehouse_sk        BIGINT,
    cs_item_sk             BIGINT NOT NULL,
    cs_promo_sk            BIGINT,
    cs_order_number        BIGINT NOT NULL,
    cs_quantity            INT,
    cs_wholesale_cost      DECIMAL(15,2),
    cs_list_price          DECIMAL(15,2),
    cs_sales_price         DECIMAL(15,2),
    cs_ext_discount_amt    DECIMAL(15,2),
    cs_ext_sales_price     DECIMAL(15,2),
    cs_ext_wholesale_cost  DECIMAL(15,2),
    cs_ext_list_price      DECIMAL(15,2),
    cs_ext_tax             DECIMAL(15,2),
    cs_coupon_amt          DECIMAL(15,2),
    cs_ext_ship_cost       DECIMAL(15,2),
    cs_net_paid            DECIMAL(15,2),
    cs_net_paid_inc_tax    DECIMAL(15,2),
    cs_net_paid_inc_ship   DECIMAL(15,2),
    cs_net_paid_inc_ship_tax DECIMAL(15,2),
    cs_net_profit          DECIMAL(15,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS catalog_returns (
    cr_returned_date_sk    BIGINT,
    cr_returned_time_sk    BIGINT,
    cr_item_sk             BIGINT NOT NULL,
    cr_refunded_customer_sk BIGINT,
    cr_refunded_cdemo_sk   BIGINT,
    cr_refunded_hdemo_sk   BIGINT,
    cr_refunded_addr_sk    BIGINT,
    cr_returning_customer_sk BIGINT,
    cr_returning_cdemo_sk  BIGINT,
    cr_returning_hdemo_sk  BIGINT,
    cr_returning_addr_sk   BIGINT,
    cr_call_center_sk      BIGINT,
    cr_catalog_page_sk     BIGINT,
    cr_ship_mode_sk        BIGINT,
    cr_warehouse_sk        BIGINT,
    cr_reason_sk           BIGINT,
    cr_order_number        BIGINT NOT NULL,
    cr_return_quantity     INT,
    cr_return_amount       DECIMAL(15,2),
    cr_return_tax          DECIMAL(15,2),
    cr_return_amt_inc_tax  DECIMAL(15,2),
    cr_fee                 DECIMAL(15,2),
    cr_return_ship_cost    DECIMAL(15,2),
    cr_refunded_cash       DECIMAL(15,2),
    cr_reversed_charge     DECIMAL(15,2),
    cr_store_credit        DECIMAL(15,2),
    cr_net_loss            DECIMAL(15,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS web_sales (
    ws_sold_date_sk        BIGINT,
    ws_sold_time_sk        BIGINT,
    ws_ship_date_sk        BIGINT,
    ws_item_sk             BIGINT NOT NULL,
    ws_bill_customer_sk    BIGINT,
    ws_bill_cdemo_sk       BIGINT,
    ws_bill_hdemo_sk       BIGINT,
    ws_bill_addr_sk        BIGINT,
    ws_ship_customer_sk    BIGINT,
    ws_ship_cdemo_sk       BIGINT,
    ws_ship_hdemo_sk       BIGINT,
    ws_ship_addr_sk        BIGINT,
    ws_web_page_sk         BIGINT,
    ws_web_site_sk         BIGINT,
    ws_ship_mode_sk        BIGINT,
    ws_warehouse_sk        BIGINT,
    ws_promo_sk            BIGINT,
    ws_order_number        BIGINT NOT NULL,
    ws_quantity            INT,
    ws_wholesale_cost      DECIMAL(15,2),
    ws_list_price          DECIMAL(15,2),
    ws_sales_price         DECIMAL(15,2),
    ws_ext_discount_amt    DECIMAL(15,2),
    ws_ext_sales_price     DECIMAL(15,2),
    ws_ext_wholesale_cost  DECIMAL(15,2),
    ws_ext_list_price      DECIMAL(15,2),
    ws_ext_tax             DECIMAL(15,2),
    ws_coupon_amt          DECIMAL(15,2),
    ws_ext_ship_cost       DECIMAL(15,2),
    ws_net_paid            DECIMAL(15,2),
    ws_net_paid_inc_tax    DECIMAL(15,2),
    ws_net_paid_inc_ship   DECIMAL(15,2),
    ws_net_paid_inc_ship_tax DECIMAL(15,2),
    ws_net_profit          DECIMAL(15,2)
) USING DELTA;

CREATE TABLE IF NOT EXISTS web_returns (
    wr_returned_date_sk    BIGINT,
    wr_returned_time_sk    BIGINT,
    wr_item_sk             BIGINT NOT NULL,
    wr_refunded_customer_sk BIGINT,
    wr_refunded_cdemo_sk   BIGINT,
    wr_refunded_hdemo_sk   BIGINT,
    wr_refunded_addr_sk    BIGINT,
    wr_returning_customer_sk BIGINT,
    wr_returning_cdemo_sk  BIGINT,
    wr_returning_hdemo_sk  BIGINT,
    wr_returning_addr_sk   BIGINT,
    wr_web_page_sk         BIGINT,
    wr_reason_sk           BIGINT,
    wr_order_number        BIGINT NOT NULL,
    wr_return_quantity     INT,
    wr_return_amt          DECIMAL(15,2),
    wr_return_tax          DECIMAL(15,2),
    wr_return_amt_inc_tax  DECIMAL(15,2),
    wr_fee                 DECIMAL(15,2),
    wr_return_ship_cost    DECIMAL(15,2),
    wr_refunded_cash       DECIMAL(15,2),
    wr_reversed_charge     DECIMAL(15,2),
    wr_account_credit      DECIMAL(15,2),
    wr_net_loss            DECIMAL(15,2)
) USING DELTA;
