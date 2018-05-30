SELECT
  count * 2 as count
FROM
  %2$s
WHERE
  _PARTITIONTIME = TIMESTAMP('%1$s')
