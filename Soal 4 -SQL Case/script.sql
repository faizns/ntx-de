-- Postgree

-- create table
CREATE TABLE IF NOT EXISTS ecommerce_session_bigquery (
    fullVisitorId VARCHAR,
    channelGrouping VARCHAR,
    country VARCHAR,
    totalTransactionRevenue VARCHAR,
    transactions VARCHAR,
    timeOnSite VARCHAR,
    pageviews VARCHAR,
    sessionQualityDim VARCHAR,
    productRefundAmount VARCHAR,
    productQuantity VARCHAR,
    productRevenue VARCHAR,
    v2ProductName VARCHAR
);

-- Change datatype
-- transactions: INTEGER
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN transactions TYPE INTEGER USING transactions::integer;

-- timeOnSite: INTEGER
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN timeOnSite TYPE INTEGER USING timeOnSite::integer;

-- pageviews: INTEGER
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN pageviews TYPE INTEGER USING pageviews::integer;

-- sessionQualityDim: INTEGER
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN sessionQualityDim TYPE INTEGER USING sessionQualityDim::integer;

-- totalTransactionRevenue: FLOAT
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN totalTransactionRevenue TYPE FLOAT USING totaltransactionrevenue::double precision;

-- productRefundAmount: FLOAT
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN productRefundAmount TYPE FLOAT USING productrefundamount::double precision;

-- productRevenue: FLOAT
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN productRevenue TYPE FLOAT USING productrevenue::double precision;

-- productQuantity: INTEGER
ALTER TABLE ecommerce_session_bigquery
ALTER COLUMN productQuantity TYPE INTEGER USING productquantity::integer;

-- --------------------------------------------------
-- CASE 1
-- --------------------------------------------------
SELECT
    channelGrouping,
    country,
    SUM(TotalTransactionRevenue) AS totalRevenue
FROM ecommerce_session_bigquery
WHERE TotalTransactionRevenue IS NOT NULL -- I don't include null data, because it affects the output result
GROUP BY channelGrouping, country
ORDER BY 3 DESC
LIMIT 5;

-- I was still confused by the request for case 1
-- so, I provided another alternative result of the top 5 countries/"country" for each "channelgrouping".

-- channel grouping distict
SELECT DISTINCT(channelgrouping)
FROM ecommerce_session_bigquery;

-- I first I create a cte table
-- I make a ranking based on the largest country revenue in each channel category.
WITH ranked_data AS (
    SELECT
        channelGrouping,
        country,
        SUM(TotalTransactionRevenue) AS totalRevenue,
        ROW_NUMBER() OVER (PARTITION BY channelGrouping ORDER BY SUM(TotalTransactionRevenue) DESC) AS rank -- create ranking, each channel groupping
    FROM ecommerce_session_bigquery
    WHERE TotalTransactionRevenue IS NOT NULL
    GROUP BY 1, 2
)
SELECT -- take 5 country for each channel
    channelGrouping,
    country,
    totalRevenue
FROM ranked_data
WHERE rank <= 5
ORDER BY 1, rank;


-- --------------------------------------------------
-- CASE 2
-- --------------------------------------------------
WITH UserMetrics AS (
    SELECT
        fullVisitorId,
        ROUND(AVG(timeOnSite),2) AS avgTimeOnSite,
        ROUND(AVG(pageviews),2) AS avgPageviews,
        ROUND(AVG(sessionQualityDim),2) AS avgSessionQualityDim
    FROM ecommerce_session_bigquery
    GROUP BY 1
)

SELECT
    U.fullVisitorId,
    U.avgTimeOnSite,
    U.avgPageviews,
    U.avgSessionQualityDim
FROM UserMetrics U
WHERE U.avgTimeOnSite > (SELECT AVG(avgTimeOnSite) FROM UserMetrics) -- spend above-average time on the site
AND U.avgPageviews < (SELECT AVG(avgPageviews) FROM UserMetrics); -- and view fewer pages than the average user


-- --------------------------------------------------
-- CASE 3
-- --------------------------------------------------
WITH ProductPerformance AS (
    SELECT
        v2ProductName AS Product,
        SUM(TotalTransactionRevenue) AS TotalRevenue,
        SUM(productQuantity) AS TotalQuantitySold,
        SUM(COALESCE(productRefundAmount, 0)) AS TotalRefundAmount -- try fill null with 0, if null, the netrevenue calculation will be null even though the total revenue has a value.
    FROM ecommerce_session_bigquery
    GROUP BY 1
)

SELECT
    P.Product,
    P.TotalRevenue,
    P.TotalRefundAmount,
    P.TotalRevenue - P.TotalRefundAmount AS NetRevenue,
    CASE  -- create new column, identified flagged or not flagged
        WHEN P.TotalRefundAmount > 0.1 * P.TotalRevenue THEN 'Flagged'
        ELSE 'Not Flagged'
    END AS RefundFlag
FROM ProductPerformance P
ORDER BY NetRevenue DESC;
