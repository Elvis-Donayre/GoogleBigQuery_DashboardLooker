-------------------------
------ Modelo 1----------
-------------------------
CREATE MODEL `bqml_gaSessions.primer_model`
OPTIONS(
  model_type='logistic_reg',
  data_split_method='RANDOM',
  data_split_eval_fraction=0.2
) AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews,
  IFNULL(totals.timeOnSite, 0) AS time_on_site,
  IFNULL(trafficSource.medium, "") AS traffic_medium
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531';

--Metricas y Evaluación con base de prueba
WITH TEST_DATA AS (
  SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    IFNULL(trafficSource.medium, "") AS traffic_medium
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
)

--Matriz de confusion
SELECT *
FROM ML.CONFUSION_MATRIX(MODEL `bqml_gaSessions.primer_model`, (SELECT * FROM TEST_DATA));

--Evaluación del modelo 
SELECT   *
FROM   ml.EVALUATE(MODEL `bqml_gaSessions.primer_model`, (SELECT * FROM TEST_DATA));

-------------------------
------ Modelo 2----------
-------------------------

CREATE MODEL `bqml_gaSessions.second_model`
OPTIONS(
    model_type='logistic_reg',
    learn_rate_strategy='line_search',
    ls_init_learn_rate=0.01,
    l2_reg=0.1,
    l1_reg=0.05,
    max_iterations=50,
    data_split_method='RANDOM',
    data_split_eval_fraction=0.2
)
AS
SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(geoNetwork.region, "") AS region,
    device.deviceCategory AS device_category,
    device.browser AS browser,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(totals.hits, 0) AS hits,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    IFNULL(trafficSource.medium, "") AS traffic_medium,
    IFNULL(trafficSource.source, "") AS traffic_source,
    IFNULL(trafficSource.campaign, "") AS campaign,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) AS visit_hour,
    IF(trafficSource.medium = 'organic', 1, 0) AS is_organic,
    IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) >= 18 AND EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) <= 23, 1, 0) AS evening_visit,
    ROUND(IFNULL(totals.timeOnSite, 0) / GREATEST(IFNULL(totals.pageviews, 0), 1), 2) AS avg_time_per_pageview
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
AND (totals.transactions IS NOT NULL OR RAND() <0.1);

--Metricas y Evaluación con base de prueba
WITH TEST_DATA AS (
SELECT
    IF(totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(device.operatingSystem, "") AS os,
    device.isMobile AS is_mobile,
    IFNULL(geoNetwork.country, "") AS country,
    IFNULL(geoNetwork.region, "") AS region,
    device.deviceCategory AS device_category,
    device.browser AS browser,
    IFNULL(totals.pageviews, 0) AS pageviews,
    IFNULL(totals.hits, 0) AS hits,
    IFNULL(totals.timeOnSite, 0) AS time_on_site,
    IFNULL(trafficSource.medium, "") AS traffic_medium,
    IFNULL(trafficSource.source, "") AS traffic_source,
    IFNULL(trafficSource.campaign, "") AS campaign,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) AS visit_hour,
    IF(trafficSource.medium = 'organic', 1, 0) AS is_organic,
    IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) >= 18 AND EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) <= 23, 1, 0) AS evening_visit,
    ROUND(IFNULL(totals.timeOnSite, 0) / GREATEST(IFNULL(totals.pageviews, 0), 1), 2) AS avg_time_per_pageview
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
AND (totals.transactions IS NOT NULL OR RAND() <0.1)
)

--Matriz de confusion
SELECT *
FROM ML.CONFUSION_MATRIX(MODEL `bqml_gaSessions.second_model`, (SELECT * FROM TEST_DATA));

--Evaluación del modelo 
SELECT   *
FROM   ml.EVALUATE(MODEL `bqml_gaSessions.second_model`, (SELECT * FROM TEST_DATA));


-------------------------
------ Modelo 3----------
-------------------------
CREATE OR REPLACE MODEL `bqml_gaSessions.fourth_model`
OPTIONS(
    model_type='random_forest_classifier',
    num_parallel_tree = 60,
    max_tree_depth = 10,
    min_split_loss = 0,
    subsample = 0.85,
    l2_reg = 0.1,
    l1_reg = 0.05,
    data_split_method = 'RANDOM',
    data_split_eval_fraction = 0.2,
    early_stop = TRUE,
    min_rel_progress = 0.001
)
AS
WITH
-- Métricas por fuente de tráfico
traffic_metrics AS (
    SELECT
        trafficSource.source AS source,
        COUNT(*) AS total_visits,
        SUM(totals.bounces) AS total_no_of_bounces,
        ROUND((SUM(totals.bounces) / COUNT(*)) * 100, 2) AS bounce_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY trafficSource.source
),

-- Métricas por dispositivo
device_metrics AS (
    SELECT
        device.operatingSystem AS operating_system,
        device.deviceCategory AS device_category,
        device.browser AS browser,
        COUNT(*) AS total_sessions_by_device
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY operating_system, device_category, browser
),

-- Métricas temporales
temporal_metrics AS (
    SELECT
        FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month,
        COUNT(*) AS total_visits_day
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY day_of_week, month
),

-- Comportamiento de usuarios
user_behavior AS (
    SELECT
        fullVisitorId,
        COUNT(*) AS visit_count
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY fullVisitorId
),

-- Conjunto de datos principal
main_dataset AS (
    SELECT
        sessions.*,
        FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) AS day_of_week,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', sessions.date)) AS month,
        CASE WHEN FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend,
        EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', sessions.date)) AS day_of_month,
        CONCAT(sessions.device.deviceCategory, '_', sessions.geoNetwork.country) AS device_country,
        CONCAT(sessions.trafficSource.medium, '_', 
               CASE 
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 0 AND 5 THEN 'night'
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 6 AND 11 THEN 'morning'
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 12 AND 17 THEN 'afternoon'
                   ELSE 'evening' 
               END) AS medium_timeofday,
        CONCAT(sessions.device.browser, '_', sessions.device.operatingSystem) AS browser_os,
        CASE 
            WHEN IFNULL(sessions.totals.timeOnSite, 0) = 0 THEN 'no_time'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 60 THEN 'very_short'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 180 THEN 'short'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 600 THEN 'medium'
            ELSE 'long'
        END AS time_on_site_category,
        IFNULL(user_behavior.visit_count, 1) AS total_visits_user,
        IFNULL(temporal_metrics.total_visits_day, 0) AS visits_day_of_week,
        IFNULL(device_metrics.total_sessions_by_device, 0) AS sessions_by_device_type
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS sessions
    LEFT JOIN user_behavior
        ON sessions.fullVisitorId = user_behavior.fullVisitorId
    LEFT JOIN temporal_metrics
        ON FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) = temporal_metrics.day_of_week
        AND EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', sessions.date)) = temporal_metrics.month
    LEFT JOIN device_metrics
        ON sessions.device.operatingSystem = device_metrics.operating_system
        AND sessions.device.deviceCategory = device_metrics.device_category
        AND sessions.device.browser = device_metrics.browser
    WHERE sessions._TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
)

-- Consulta final para el entrenamiento del modelo
SELECT
    -- Etiqueta objetivo
    IF(main.totals.transactions IS NULL, 0, 1) AS label,
    
    -- Características originales
    IFNULL(main.device.operatingSystem, '') AS os,
    main.device.isMobile AS is_mobile,
    IFNULL(main.geoNetwork.country, '') AS country,
    IFNULL(main.geoNetwork.region, '') AS region,
    main.device.deviceCategory AS device_category,
    main.device.browser AS browser,
    IFNULL(main.totals.pageviews, 0) AS pageviews,
    IFNULL(main.totals.hits, 0) AS hits,
    IFNULL(main.totals.timeOnSite, 0) AS time_on_site,
    IFNULL(main.trafficSource.medium, '') AS traffic_medium,
    IFNULL(main.trafficSource.source, '') AS traffic_source,
    IFNULL(main.trafficSource.campaign, '') AS campaign,
    
    -- Características temporales
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(main.visitStartTime)) AS visit_hour,
    main.day_of_week,
    main.month,
    main.is_weekend,
    main.day_of_month,
    
    -- Características de comportamiento
    main.time_on_site_category,
    traffic_metrics.bounce_rate,
    main.sessions_by_device_type,
    IF(main.trafficSource.medium = 'organic', 1, 0) AS is_organic,
    ROUND(IFNULL(main.totals.timeOnSite, 0) / GREATEST(IFNULL(main.totals.pageviews, 0), 1), 2) AS avg_time_per_pageview,
    
    -- Características de recurrencia
    main.total_visits_user,
    
    -- Características de usuario y dispositivo
    main.device_country,
    main.medium_timeofday,
    main.browser_os,
    main.visits_day_of_week,
    
    -- Características adicionales de navegación
    ROUND(IFNULL(main.totals.pageviews, 0) / GREATEST(IFNULL(main.totals.hits, 0), 1), 3) AS pageviews_per_hit
FROM main_dataset AS main
LEFT JOIN traffic_metrics
    ON main.trafficSource.source = traffic_metrics.source
AND (main.totals.transactions IS NOT NULL OR RAND() < 0.05);

--------------------------------------------
--Metricas y Evaluación con base de prueba--
--------------------------------------------
WITH TEST_DATA AS (
WITH
-- Métricas por fuente de tráfico
traffic_metrics AS (
    SELECT
        trafficSource.source AS source,
        COUNT(*) AS total_visits,
        SUM(totals.bounces) AS total_no_of_bounces,
        ROUND((SUM(totals.bounces) / COUNT(*)) * 100, 2) AS bounce_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY trafficSource.source
),

-- Métricas por dispositivo
device_metrics AS (
    SELECT
        device.operatingSystem AS operating_system,
        device.deviceCategory AS device_category,
        device.browser AS browser,
        COUNT(*) AS total_sessions_by_device
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY operating_system, device_category, browser
),

-- Métricas temporales
temporal_metrics AS (
    SELECT
        FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', date)) AS day_of_week,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', date)) AS month,
        COUNT(*) AS total_visits_day
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY day_of_week, month
),

-- Comportamiento de usuarios
user_behavior AS (
    SELECT
        fullVisitorId,
        COUNT(*) AS visit_count
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY fullVisitorId
),

-- Conjunto de datos principal
main_dataset AS (
    SELECT
        sessions.*,
        FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) AS day_of_week,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', sessions.date)) AS month,
        CASE WHEN FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS is_weekend,
        EXTRACT(DAY FROM PARSE_DATE('%Y%m%d', sessions.date)) AS day_of_month,
        CONCAT(sessions.device.deviceCategory, '_', sessions.geoNetwork.country) AS device_country,
        CONCAT(sessions.trafficSource.medium, '_', 
               CASE 
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 0 AND 5 THEN 'night'
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 6 AND 11 THEN 'morning'
                   WHEN EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) BETWEEN 12 AND 17 THEN 'afternoon'
                   ELSE 'evening' 
               END) AS medium_timeofday,
        CONCAT(sessions.device.browser, '_', sessions.device.operatingSystem) AS browser_os,
        CASE 
            WHEN IFNULL(sessions.totals.timeOnSite, 0) = 0 THEN 'no_time'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 60 THEN 'very_short'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 180 THEN 'short'
            WHEN IFNULL(sessions.totals.timeOnSite, 0) < 600 THEN 'medium'
            ELSE 'long'
        END AS time_on_site_category,
        IFNULL(user_behavior.visit_count, 1) AS total_visits_user,
        IFNULL(temporal_metrics.total_visits_day, 0) AS visits_day_of_week,
        IFNULL(device_metrics.total_sessions_by_device, 0) AS sessions_by_device_type
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS sessions
    LEFT JOIN user_behavior
        ON sessions.fullVisitorId = user_behavior.fullVisitorId
    LEFT JOIN temporal_metrics
        ON FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', sessions.date)) = temporal_metrics.day_of_week
        AND EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', sessions.date)) = temporal_metrics.month
    LEFT JOIN device_metrics
        ON sessions.device.operatingSystem = device_metrics.operating_system
        AND sessions.device.deviceCategory = device_metrics.device_category
        AND sessions.device.browser = device_metrics.browser
    WHERE sessions._TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
)

-- Consulta final para el entrenamiento del modelo
SELECT
    -- Etiqueta objetivo
    IF(main.totals.transactions IS NULL, 0, 1) AS label,
    
    -- Características originales
    IFNULL(main.device.operatingSystem, '') AS os,
    main.device.isMobile AS is_mobile,
    IFNULL(main.geoNetwork.country, '') AS country,
    IFNULL(main.geoNetwork.region, '') AS region,
    main.device.deviceCategory AS device_category,
    main.device.browser AS browser,
    IFNULL(main.totals.pageviews, 0) AS pageviews,
    IFNULL(main.totals.hits, 0) AS hits,
    IFNULL(main.totals.timeOnSite, 0) AS time_on_site,
    IFNULL(main.trafficSource.medium, '') AS traffic_medium,
    IFNULL(main.trafficSource.source, '') AS traffic_source,
    IFNULL(main.trafficSource.campaign, '') AS campaign,
    
    -- Características temporales
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(main.visitStartTime)) AS visit_hour,
    main.day_of_week,
    main.month,
    main.is_weekend,
    main.day_of_month,
    
    -- Características de comportamiento
    main.time_on_site_category,
    traffic_metrics.bounce_rate,
    main.sessions_by_device_type,
    IF(main.trafficSource.medium = 'organic', 1, 0) AS is_organic,
    ROUND(IFNULL(main.totals.timeOnSite, 0) / GREATEST(IFNULL(main.totals.pageviews, 0), 1), 2) AS avg_time_per_pageview,
    
    -- Características de recurrencia
    main.total_visits_user,
    
    -- Características de usuario y dispositivo
    main.device_country,
    main.medium_timeofday,
    main.browser_os,
    main.visits_day_of_week,
    
    -- Características adicionales de navegación
    ROUND(IFNULL(main.totals.pageviews, 0) / GREATEST(IFNULL(main.totals.hits, 0), 1), 3) AS pageviews_per_hit
FROM main_dataset AS main
LEFT JOIN traffic_metrics
    ON main.trafficSource.source = traffic_metrics.source
AND (main.totals.transactions IS NOT NULL OR RAND() < 0.05)
)
--Evaluación del modelo 
SELECT   *
FROM   ml.EVALUATE(MODEL `bqml_gaSessions.fourth_model`, (SELECT * FROM TEST_DATA), STRUCT(0.5 AS threshold));

--Matriz de confusion
SELECT *
FROM ML.CONFUSION_MATRIX(MODEL `bqml_gaSessions.fourth_model`, (SELECT * FROM TEST_DATA), STRUCT(0.5 AS threshold));


-------------------------
------ Modelo 4----------
-------------------------
CREATE MODEL `bqml_gaSessions.third_model`
OPTIONS(
    model_type='random_forest_classifier',
    num_parallel_tree =60,
    max_tree_depth=10,
    min_split_loss=0,
    subsample=0.85,
    l2_reg=0.1,
    l1_reg=0.05,
    data_split_method='RANDOM',
    data_split_eval_fraction=0.2
)
AS
WITH
traffic_metrics AS (
    SELECT
        trafficSource.source AS source,
        COUNT(trafficSource.source) AS total_visits,
        SUM(totals.bounces) AS total_no_of_bounces,
        ROUND((SUM(totals.bounces) / COUNT(trafficSource.source)) * 100, 2) AS bounce_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY 1
),
conversion_metrics AS (
    SELECT
        device.operatingSystem AS operating_system,
        device.deviceCategory AS device_category,
        device.browser AS browser,
        COUNT(*) AS total_sessions,
        SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions,
        ROUND((SUM(IF(totals.transactions IS NULL, 0, 1)) / COUNT(*)) * 100, 2) AS conversion_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
    GROUP BY 1, 2, 3
)
SELECT
    IF(sessions.totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(sessions.device.operatingSystem, "") AS os,
    sessions.device.isMobile AS is_mobile,
    IFNULL(sessions.geoNetwork.country, "") AS country,
    IFNULL(sessions.geoNetwork.region, "") AS region,
    sessions.device.deviceCategory AS device_category,
    sessions.device.browser AS browser,
    IFNULL(sessions.totals.pageviews, 0) AS pageviews,
    IFNULL(sessions.totals.hits, 0) AS hits,
    IFNULL(sessions.totals.timeOnSite, 0) AS time_on_site,
    IFNULL(sessions.trafficSource.medium, "") AS traffic_medium,
    IFNULL(sessions.trafficSource.source, "") AS traffic_source,
    IFNULL(sessions.trafficSource.campaign, "") AS campaign,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) AS visit_hour,
    traffic_metrics.bounce_rate,
    conversion_metrics.conversion_rate,
    IF(sessions.trafficSource.medium = 'organic', 1, 0) AS is_organic,
    IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) >= 18 AND EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) <= 23, 1, 0) AS evening_visit,
    ROUND(IFNULL(sessions.totals.timeOnSite, 0) / GREATEST(IFNULL(sessions.totals.pageviews, 0), 1), 2) AS avg_time_per_pageview,
    (SELECT COUNT(*) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS inner_sessions
     WHERE inner_sessions.fullVisitorId = sessions.fullVisitorId
     AND PARSE_DATE('%Y%m%d', inner_sessions.date) < PARSE_DATE('%Y%m%d', sessions.date)) AS visit_frequency,
    (SELECT SUM(totals.transactionRevenue) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS inner_sessions
     WHERE inner_sessions.fullVisitorId = sessions.fullVisitorId
     AND PARSE_DATE('%Y%m%d', inner_sessions.date) < PARSE_DATE('%Y%m%d', sessions.date)) AS total_transaction_value,
    PARSE_DATE('%Y%m%d', sessions.date) AS visit_date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS sessions
LEFT JOIN traffic_metrics
ON sessions.trafficSource.source = traffic_metrics.source
LEFT JOIN conversion_metrics
ON sessions.device.operatingSystem = conversion_metrics.operating_system
AND sessions.device.deviceCategory = conversion_metrics.device_category
AND sessions.device.browser = conversion_metrics.browser
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170531'
AND (sessions.totals.transactions IS NOT NULL OR RAND() < 0.05);


--Metricas y Evaluación con base de prueba
WITH TEST_DATA AS (
WITH
traffic_metrics AS (
    SELECT
        trafficSource.source AS source,
        COUNT(trafficSource.source) AS total_visits,
        SUM(totals.bounces) AS total_no_of_bounces,
        ROUND((SUM(totals.bounces) / COUNT(trafficSource.source)) * 100, 2) AS bounce_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY 1
),
conversion_metrics AS (
    SELECT
        device.operatingSystem AS operating_system,
        device.deviceCategory AS device_category,
        device.browser AS browser,
        COUNT(*) AS total_sessions,
        SUM(IF(totals.transactions IS NULL, 0, 1)) AS total_transactions,
        ROUND((SUM(IF(totals.transactions IS NULL, 0, 1)) / COUNT(*)) * 100, 2) AS conversion_rate
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
    GROUP BY 1, 2, 3
)
SELECT
    IF(sessions.totals.transactions IS NULL, 0, 1) AS label,
    IFNULL(sessions.device.operatingSystem, "") AS os,
    sessions.device.isMobile AS is_mobile,
    IFNULL(sessions.geoNetwork.country, "") AS country,
    IFNULL(sessions.geoNetwork.region, "") AS region,
    sessions.device.deviceCategory AS device_category,
    sessions.device.browser AS browser,
    IFNULL(sessions.totals.pageviews, 0) AS pageviews,
    IFNULL(sessions.totals.hits, 0) AS hits,
    IFNULL(sessions.totals.timeOnSite, 0) AS time_on_site,
    IFNULL(sessions.trafficSource.medium, "") AS traffic_medium,
    IFNULL(sessions.trafficSource.source, "") AS traffic_source,
    IFNULL(sessions.trafficSource.campaign, "") AS campaign,
    EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) AS visit_hour,
    traffic_metrics.bounce_rate,
    conversion_metrics.conversion_rate,
    IF(sessions.trafficSource.medium = 'organic', 1, 0) AS is_organic,
    IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) >= 18 AND EXTRACT(HOUR FROM TIMESTAMP_SECONDS(sessions.visitStartTime)) <= 23, 1, 0) AS evening_visit,
    ROUND(IFNULL(sessions.totals.timeOnSite, 0) / GREATEST(IFNULL(sessions.totals.pageviews, 0), 1), 2) AS avg_time_per_pageview,
    (SELECT COUNT(*) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS inner_sessions
     WHERE inner_sessions.fullVisitorId = sessions.fullVisitorId
     AND PARSE_DATE('%Y%m%d', inner_sessions.date) < PARSE_DATE('%Y%m%d', sessions.date)) AS visit_frequency,
    (SELECT SUM(totals.transactionRevenue) 
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS inner_sessions
     WHERE inner_sessions.fullVisitorId = sessions.fullVisitorId
     AND PARSE_DATE('%Y%m%d', inner_sessions.date) < PARSE_DATE('%Y%m%d', sessions.date)) AS total_transaction_value,
    PARSE_DATE('%Y%m%d', sessions.date) AS visit_date
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` AS sessions
LEFT JOIN traffic_metrics
ON sessions.trafficSource.source = traffic_metrics.source
LEFT JOIN conversion_metrics
ON sessions.device.operatingSystem = conversion_metrics.operating_system
AND sessions.device.deviceCategory = conversion_metrics.device_category
AND sessions.device.browser = conversion_metrics.browser
WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
AND (sessions.totals.transactions IS NOT NULL OR RAND() < 0.05)
)

--Evaluación del modelo 
SELECT   *
FROM   ml.EVALUATE(MODEL `bqml_gaSessions.third_model`, (SELECT * FROM TEST_DATA), STRUCT(0.5 AS threshold));

--Matriz de confusion
SELECT *
FROM ML.CONFUSION_MATRIX(MODEL `bqml_gaSessions.third_model`, (SELECT * FROM TEST_DATA), STRUCT(0.5 AS threshold));

