-- ganti rentang sesuai kebutuhan
WITH dates AS (
  SELECT generate_series('2020-01-01'::date, CURRENT_DATE, '1 day') AS d
)
INSERT INTO core.calendar (trading_date, is_open)
SELECT d, CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN FALSE ELSE TRUE END
FROM dates
ON CONFLICT (trading_date) DO NOTHING;
