CREATE OR REPLACE TABLE `beaming-surfer-440101-i4.bqml_gaSessions.daily_predictions` (
  unique_session_id STRING,
  label INT64,
  operatingSystem STRING,
  isMobile BOOLEAN,
  country STRING,
  region STRING,
  deviceCategory STRING,
  browser STRING,
  pageViews INT64,
  hits INT64,
  timeOnSite INT64,
  trafficMedium STRING,
  trafficSource STRING,
  campaign STRING,
  visitHour INT64,
  isOrganic INT64,
  eveningVisit INT64,
  avgTimePerPageView FLOAT64,
  bounceRate FLOAT64,
  conversionRate FLOAT64,
  visitFrequency INT64,
  totalTransactionValue FLOAT64,
  predictedLabel INT64,
  predictedProbability FLOAT64,
  predictionDate DATE
)
PARTITION BY predictionDate;

INSERT INTO `beaming-surfer-440101-i4.bqml_gaSessions.daily_predictions`
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
),
predictions AS (
    SELECT
        CONCAT(fullVisitorId, '-', visitId) AS unique_session_id,
        predicted_label,
        predicted_label_probs[OFFSET(0)].prob AS predicted_probability,
        label,
        os,
        is_mobile,
        country,
        region,
        device_category,
        browser,
        pageviews,
        hits,
        time_on_site,
        traffic_medium,
        traffic_source,
        campaign,
        visit_hour,
        is_organic,
        evening_visit,
        avg_time_per_pageview,
        bounce_rate,
        conversion_rate,
        visit_frequency,
        total_transaction_value,
        visit_date
    FROM
        ML.PREDICT(MODEL `beaming-surfer-440101-i4.bqml_gaSessions.third_model`,
            (
            SELECT
                sessions.fullVisitorId,
                sessions.visitId,
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
                IF(EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) >= 18 AND
                   EXTRACT(HOUR FROM TIMESTAMP_SECONDS(visitStartTime)) <= 23, 1, 0) AS evening_visit,
                ROUND(IFNULL(totals.timeOnSite, 0) / GREATEST(IFNULL(totals.pageviews, 0), 1), 2) AS avg_time_per_pageview,
                traffic_metrics.bounce_rate,
                conversion_metrics.conversion_rate,
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
            WHERE _TABLE_SUFFIX BETWEEN '20170601' AND '20170801'
            )
        ) ORDER BY predicted_probability DESC
)
SELECT
    unique_session_id,
    label,
    os AS operatingSystem,
    is_mobile AS isMobile,
    country,
    region,
    device_category AS deviceCategory,
    browser,
    pageviews AS pageViews,
    hits,
    time_on_site AS timeOnSite,
    traffic_medium AS trafficMedium,
    traffic_source AS trafficSource,
    campaign,
    visit_hour AS visitHour,
    is_organic AS isOrganic,
    evening_visit AS eveningVisit,
    avg_time_per_pageview AS avgTimePerPageView,
    bounce_rate AS bounceRate,
    conversion_rate AS conversionRate,
    visit_frequency AS visitFrequency,
    total_transaction_value AS totalTransactionValue,
    predicted_label AS predictedLabel,
    predicted_probability AS predictedProbability,
    CURRENT_DATE() AS predictionDate
FROM predictions;