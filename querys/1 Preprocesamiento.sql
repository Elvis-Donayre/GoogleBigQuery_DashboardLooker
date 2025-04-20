--Conociendo los datos de una partición (día)
SELECT *
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
LIMIT 100;

--Consultando los datos de todas las particiones
SELECT *
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _TABLE_SUFFIX BETWEEN '20160801' AND '20170801'
AND visitorId IS NOT NULL;

--Convirtiendo Diccionario a Columnas
SELECT fullVisitorId,
        visitId,
        hit.hitNumber,
        hit.page.pagePath
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
      UNNEST(hits) AS hit
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170131';
