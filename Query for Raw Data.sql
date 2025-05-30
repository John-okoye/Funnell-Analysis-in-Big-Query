WITH events AS (
  SELECT 
    event_timestamp,
    event_name,
    user_pseudo_id,
    ROW_NUMBER() OVER (
      PARTITION BY event_name, user_pseudo_id ORDER BY event_timestamp) AS serial_no
  FROM `turing_data_analytics.raw_events`
  GROUP BY ALL
),

earliest_events AS (
  SELECT
    e.user_pseudo_id,
    MIN(e.event_timestamp) AS first_event_timestamp,
    e.event_name,
    raw.country
  FROM events e
  JOIN
    `turing_data_analytics.raw_events` raw
  ON
    e.user_pseudo_id = raw.user_pseudo_id
  AND
    e.event_timestamp = raw.event_timestamp
  WHERE e.serial_no = 1
  GROUP BY e.user_pseudo_id, e.event_name, raw.country
),

top_countries AS (
  SELECT
    country,
    COUNT(*) AS event_count
FROM
    earliest_events
GROUP BY
    country
ORDER BY
    event_count DESC
LIMIT 3
)

SELECT
earliest.country,
earliest.event_name,
COUNT(*) AS event_count
FROM
earliest_events earliest
JOIN
top_countries top
ON
earliest.country = top.country
WHERE event_name IN (
  "session_start", 
  "add_to_cart", 
  "begin_checkout", 
  "add_payment_info", 
  "purchase"
)
GROUP BY
earliest.country, earliest.event_name 
ORDER BY
earliest.country, event_count DESC
