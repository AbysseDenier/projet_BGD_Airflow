{{ config(
    materialized='table',
    schema='bigdata_dbt_bigdata_dbt'
) }}

SELECT
    sp500.date,
    COALESCE(btc.btc_price, NULL) AS btc_price, 
    COALESCE(btc.btc_market_cap, NULL) AS btc_market_cap,
    COALESCE(btc.btc_volume, NULL) AS btc_volume,
    sp500.sp500_price
FROM {{ ref('stg_sp500') }} sp500
LEFT JOIN {{ ref('stg_btc') }} btc
ON sp500.date = btc.date
