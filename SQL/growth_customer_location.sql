WITH params AS (
    -- Define the date parameters here
    SELECT 
        DATE('2024-01-01') AS start_date,
        DATE('2024-07-01') AS end_date
),
  location_counts AS (
  SELECT
    customer_id,
    latitude,
    longitude,
    COUNT(*) frequency
  FROM
    `risk_mart.customer_gps_location`
  JOIN params p on SourceAsofDate BETWEEN p.start_date and p.end_date
  WHERE event_description = 'Apigee Logs' AND latitude <> 'undefined' AND longitude <> 'undefined'
  GROUP BY ALL
  ),
  frequent_location AS (
  SELECT
    customer_id,
    latitude,
    longitude,
  FROM
    location_counts
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY frequency DESC) = 1)
SELECT
  frequent_location.*,
  country,
  state,
  county,
  city,
  municipality,
  postcode,
  road,
  state_code,
  result_type,
  formatted,
  address_line1,
  address_line2
FROM
  frequent_location
LEFT JOIN (
  SELECT
    *
  FROM
    dl_customers_db_derived.customer_address_derived_geocoding
  JOIN params p on date(date_time) BETWEEN p.start_date and p.end_date
  --WHERE date(date_time) BETWEEN "2024-01-01" AND '2024-07-01'
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY customer_id, latitude, longitude ORDER BY date_time DESC) = 1) address
ON
  address.customer_id = frequent_location.customer_id
  AND address.latitude = frequent_location.latitude
  AND address.longitude = frequent_location.longitude