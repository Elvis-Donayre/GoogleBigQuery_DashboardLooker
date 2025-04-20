# Diccionario de la tabla `ga_sessions`

La tabla `ga_sessions` contiene datos de sesiones de Google Analytics exportados a BigQuery. A continuación, se describen los principales campos de esta tabla:

- `visitorId`: Identificador único del visitante.
- `visitId`: Identificador único de la sesión.
- `visitNumber`: Número de visitas realizadas por el visitante.
- `visitStartTime`: Marca de tiempo del inicio de la sesión.
- `date`: Fecha de la sesión en formato AAAAMMDD.
- `totals`: Estructura que contiene métricas agregadas de la sesión, como:
  - `pageviews`: Número de páginas vistas durante la sesión.
  - `timeOnSite`: Tiempo total en el sitio durante la sesión.
  - `transactions`: Número de transacciones realizadas durante la sesión.
  - `transactionRevenue`: Ingresos generados por las transacciones de la sesión.
- `trafficSource`: Estructura que contiene información sobre la fuente de tráfico de la sesión, incluyendo:
  - `source`: Origen del tráfico (por ejemplo, "google").
  - `medium`: Medio del tráfico (por ejemplo, "organic").
  - `campaign`: Nombre de la campaña asociada al tráfico.
- `device`: Estructura que proporciona detalles sobre el dispositivo utilizado durante la sesión, como:
  - `category`: Tipo de dispositivo (por ejemplo, "desktop", "mobile").
  - `operatingSystem`: Sistema operativo del dispositivo.
  - `browser`: Navegador utilizado durante la sesión.
- `geoNetwork`: Estructura que contiene información geográfica del visitante, incluyendo:
  - `continent`: Continente de origen.
  - `country`: País de origen.
  - `city`: Ciudad de origen.
- `hits`: Array que registra las interacciones individuales del usuario durante la sesión, como páginas vistas o eventos. Cada elemento del array puede contener:
  - `type`: Tipo de interacción (por ejemplo, "PAGE", "EVENT").
  - `page`: Información sobre la página vista, incluyendo:
    - `pagePath`: Ruta de la página.
    - `pageTitle`: Título de la página.
  - `eventInfo`: Detalles del evento, como:
    - `eventCategory`: Categoría del evento.
    - `eventAction`: Acción del evento.
    - `eventLabel`: Etiqueta del evento.
  - `product`: Información sobre productos asociados a la interacción, incluyendo:
    - `productSKU`: Identificador del producto.
    - `productName`: Nombre del producto.
    - `productCategory`: Categoría del producto.
    - `productRevenue`: Ingresos generados por el producto.

**Nota**: La estructura de la tabla es compleja y contiene múltiples campos anidados y repetidos. Para una comprensión más detallada del esquema, se recomienda consultar la documentación oficial de Google Analytics y BigQuery.
