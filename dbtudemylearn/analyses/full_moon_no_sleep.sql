WITH mart_fullmoon_review AS (
    SELECT * FROM {{ ref('mart_fullmoon_review') }}
)
SELECT 
    is_full_moon,
    review_sentiment,
    COUNT(*) as reviews
FROM 
    mart_fullmoon_reviews
GROUP BY
    is_full_moon,
    review_sentiment
ORDER BY
    is_full_moon,
    review_sentiment