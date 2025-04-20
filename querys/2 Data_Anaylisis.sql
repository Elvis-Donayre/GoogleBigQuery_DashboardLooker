--Cual es el número total de transacciones generadas por navegador y tipo de dispositivo?
SELECT device.deviceCategory,
       device.browser,
       SUM(totals.transactions) AS total_transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161101' AND '20161130'
GROUP BY 1,2
HAVING SUM(totals.transactions) IS NOT NULL
ORDER BY 1, 3 DESC;

--Cual es el porcentaje de rechazo por origen de tráfico?
SELECT source,
        total_visits,
        total_bounces,
        ((total_bounces / total_visits ) * 100 ) AS bounce_rate
FROM (SELECT trafficSource.source AS source,
             COUNT ( trafficSource.source ) AS total_visits,
             SUM ( totals.bounces ) AS total_bounces
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE _TABLE_SUFFIX BETWEEN '20170701' AND '20170731'
      GROUP BY 1
      )
ORDER BY 1 DESC;

--Cual es el porcentaje de conversión por operating_system,device_category y browser?
SELECT
    IFNULL(device.operatingSystem, 'Unknown') AS operating_system,
    IFNULL(device.deviceCategory, 'Unknown') AS device_category,
    IFNULL(device.browser, 'Unknown') AS browser,
    COUNT(*) AS total_sessions,
    SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions,
    ROUND((SUM(IF(totals.transactions IS NULL, 0, 1)) / COUNT(*)) * 100, 2) AS conversion_rate_percentage
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1, 2, 3
HAVING conversion_rate_percentage > 1
ORDER BY 6 DESC;

--Cual es el porcentaje de visitantes que hizo una compra en el sitio web?
SELECT
  total_visitors,
  total_purchasers,
  ROUND((total_purchasers / total_visitors * 100), 2) AS conversion_rate_pct
FROM
(
SELECT COUNT(DISTINCT fullVisitorId) AS total_visitors
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161101' AND '20161130'
) AS visitors,
(
SELECT COUNT(DISTINCT fullVisitorId) AS total_purchasers
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161101' AND '20161130'
AND totals.transactions IS NOT NULL
) AS purchasers;


--Análisis de ingresos por fuente de tráfico y medio
SELECT
    trafficSource.source AS source,
    trafficSource.medium AS medium,
    COUNT(*) AS total_sessions,
    SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions,
    ROUND(SUM(IFNULL(totals.transactionRevenue, 0)) / 1000000, 2) AS total_revenue_usd,
    ROUND((SUM(IF(totals.transactions IS NULL, 0, 1)) / COUNT(*)) * 100, 2) AS conversion_rate_percentage
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1, 2
HAVING conversion_rate_percentage >1
ORDER BY total_revenue_usd DESC;

--Países con mayor tasa de rebote
SELECT
geoNetwork.country AS country,
    COUNT(*) AS total_sessions,
    SUM(IFNULL(totals.bounces, 0)) AS total_bounces,
    ROUND((SUM(IFNULL(totals.bounces, 0)) / COUNT(*)) * 100, 2) AS bounce_rate_percentage
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1
HAVING total_sessions > 100
ORDER BY bounce_rate_percentage DESC
LIMIT 10;


--Tiempo promedio por dispositivo
SELECT
    device.deviceCategory AS device_category,
    COUNT(*) AS total_sessions,
    ROUND(AVG(IFNULL(totals.timeOnSite, 0)), 2) AS avg_time_on_site_seconds
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1
ORDER BY avg_time_on_site_seconds DESC;

--Comparación de campañas de marketing
SELECT
    trafficSource.campaign AS campaign,
    COUNT(*) AS total_sessions,
    SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions,
    ROUND(SUM(IFNULL(totals.transactionRevenue, 0)) / 1000000, 2) AS total_revenue_usd,
    ROUND((SUM(IF(totals.transactions IS NULL, 0, 1)) / COUNT(*)) * 100, 2) AS conversion_rate_percentage
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
    AND trafficSource.campaign IS NOT NULL
    AND trafficSource.campaign != '(not set)'  -- Excluye campañas no definidas
GROUP BY 1
ORDER BY total_revenue_usd DESC;

--Análisis de páginas vistas por navegador
SELECT
    device.browser AS browser,
    COUNT(*) AS total_sessions,
    ROUND(AVG(IFNULL(totals.pageviews, 0)), 2) AS avg_pageviews_per_session
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1
HAVING total_sessions > 100
ORDER BY avg_pageviews_per_session DESC;

--Sesiones por día de la semana
SELECT
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 1 THEN 'Sunday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 2 THEN 'Monday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 3 THEN 'Tuesday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 4 THEN 'Wednesday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 5 THEN 'Thursday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 6 THEN 'Friday'
        WHEN EXTRACT(DAYOFWEEK FROM PARSE_DATE('%Y%m%d', date)) = 7 THEN 'Saturday'
    END AS day_of_week,
    COUNT(*) AS total_sessions,
    SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20161001' AND '20161130'
GROUP BY 1
ORDER BY total_transactions DESC;
