WITH sp500 AS (
    SELECT
        date,
        price AS sp500_price
    FROM {{ source('my_sources', 'sp500_concatenated') }}
)

SELECT * FROM sp500
