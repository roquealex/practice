CREATE EXTERNAL TABLE inter
(timestamp INT,reading INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/user/cloudera/inter/'
TBLPROPERTIES ("skip.header.line.count"="1");

SET hivevar:FIVE_MIN=5;

ADD jar ./sequenceUdf.jar;
CREATE TEMPORARY FUNCTION sequence AS 'SequenceUDF';

SELECT *, sequence(range_start,range_stop,${FIVE_MIN})
FROM (
SELECT *,
  ((timestamp+${FIVE_MIN}-1) DIV ${FIVE_MIN})*${FIVE_MIN} as range_start,
  (next_timestamp-1) as range_stop,

FROM (
  SELECT *,
    LEAD(timestamp) OVER(PARTITION BY hundreds ORDER BY timestamp) AS next_timestamp,
    LEAD(reading) OVER(PARTITION BY hundreds ORDER BY timestamp) AS next_reading
  FROM (
    SELECT timestamp DIV 100 AS hundreds, * FROM inter
  ) h_tb
) w_tb
WHERE next_timestamp IS NOT NULL
) r_tb;

DROP TABLE inter;



