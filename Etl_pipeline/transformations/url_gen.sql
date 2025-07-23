
CREATE MATERIALIZED VIEW mycatalog.hp_prd_data.generated_urls_mv
PARTITIONED BY (processed_datehour)
AS
SELECT
  zip,
  state_id,
  state_name,
  format_string(
    '${api_url}',
    zip
  ) AS api_url,
  format_string(
   '${referal_url}',
    zip, state_id, state_name
  ) as refereral_url,
  date_format(current_timestamp(), 'yyyy-MM-dd-HH') AS processed_datehour
FROM (
    SELECT 
      zip,
      state_id,
      REPLACE(state_name, ' ', '-') AS state_name, 
      population
    FROM mycatalog.hp_prd_data.uszips
    WHERE population IS NOT NULL 
    ORDER BY rand()
    LIMIT ${zip_load_count}
) AS sub