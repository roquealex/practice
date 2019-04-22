CREATE EXTERNAL TABLE IF NOT EXISTS inter
(ts INT,reading INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/inter/'
TBLPROPERTIES ("skip.header.line.count"="1");

SET hivevar:FIVE_MIN=5;

ADD jar ./sequenceUdf.jar;
CREATE TEMPORARY FUNCTION sequence AS 'SequenceUDF';

SELECT explode(array_ts)
FROM (
SELECT *, sequence(range_start,range_stop,${FIVE_MIN}) as array_ts
FROM (
SELECT *,
  ((ts+${FIVE_MIN}-1) DIV ${FIVE_MIN})*${FIVE_MIN} as range_start,
  (next_ts-1) as range_stop
FROM (
  SELECT *,
    LEAD(ts) OVER(PARTITION BY hundreds ORDER BY ts) AS next_ts,
    LEAD(reading) OVER(PARTITION BY hundreds ORDER BY ts) AS next_reading
  FROM (
    SELECT ts DIV 100 AS hundreds, * FROM inter
  ) h_tb
) w_tb
WHERE next_ts IS NOT NULL
) r_tb
) x;

DROP TABLE inter;



