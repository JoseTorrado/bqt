SELECT 
    id,
    UPPER(name) AS upper_name,
    LENGTH(name) AS name_length
  FROM mytable
  WHERE
    status is NULL
