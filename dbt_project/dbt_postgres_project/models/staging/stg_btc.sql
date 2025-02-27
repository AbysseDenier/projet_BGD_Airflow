WITH btc AS (
    SELECT
        date,
        price AS btc_price,
        market_cap AS btc_market_cap,
        volume AS btc_volume
    FROM {{ source('my_sources', 'btc_concatenated') }}
)

SELECT * FROM btc
