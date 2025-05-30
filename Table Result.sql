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
),

funnel_raw AS (SELECT
earliest.country,
earliest.event_name,
COUNT(*) AS event_count
FROM
earliest_events earliest
JOIN
top_countries top
ON
earliest.country = top.country
GROUP BY
earliest.country, earliest.event_name
ORDER BY
earliest.country, event_count DESC
),

pivoted AS (
  SELECT 
    event_name,
    MAX(CASE WHEN country = 'Canada' THEN event_count END) AS country1_events,
    MAX(CASE WHEN country = 'United States' THEN event_count END) AS country2_events,
    MAX(CASE WHEN country = 'India' THEN event_count END) AS country3_events
  FROM funnel_raw
  GROUP BY event_name
),

with_totals AS (
  SELECT 
    event_name,
    country1_events,
    country2_events,
    country3_events,
    (country1_events + country2_events + country3_events) AS total_events
  FROM pivoted
),

ranked AS (
  SELECT 
    ROW_NUMBER() OVER (ORDER BY total_events DESC) AS event_order,
    *
  FROM with_totals
),

top_event AS (
  SELECT 
    country1_events AS top_c1,
    country2_events AS top_c2,
    country3_events AS top_c3,
    (country1_events + country2_events + country3_events) AS top_total
  FROM ranked
  WHERE event_order = 1
)

SELECT 
  r.event_order,
  r.event_name,
  r.country1_events AS `1st Country events`,
  r.country2_events AS `2nd Country events`,
  r.country3_events AS `3rd Country events`,
  ROUND(100 * r.total_events / t.top_total, 2) AS Full_perc,
  ROUND(100 * (t.top_c1 - r.country1_events) / t.top_c1, 2) AS `1st_country_perc_drop`,
  ROUND(100 * (t.top_c2 - r.country2_events) / t.top_c2, 2) AS `2nd_country_perc_drop`,
  ROUND(100 * (t.top_c3 - r.country3_events) / t.top_c3, 2) AS `3rd_country_perc_drop`
FROM ranked r
CROSS JOIN top_event t
WHERE event_name IN (
  "session_start",  
  "add_to_cart", 
  "begin_checkout", 
  "add_payment_info", 
  "purchase"
)
ORDER BY event_order;


































































































