WITH sp500 AS (
    SELECT * FROM {{ ref('stg_sp500') }}
),

btc AS (
    SELECT * FROM {{ ref('stg_btc') }}
),

final AS (
    SELECT
        sp500.date,
        sp500.sp500_price,
        btc.btc_price,
        btc.btc_market_cap,
        btc.btc_volume
    FROM sp500
    LEFT JOIN btc ON sp500.date = btc.date  -- SP500 dates as reference
)

SELECT * FROM final
