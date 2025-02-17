WITH category_stats AS (
    SELECT
      category,
      COUNT(*) AS cnt,
      ROUND(AVG(price), 2) AS avg_price
    FROM mytable
    GROUP BY category
  )
  SELECT
    m.id,
    m.category,
    m.price,
    cs.cnt,
    cs.avg_price,
    CASE
      WHEN m.price > cs.avg_price THEN 'Above Average'
      ELSE 'Below Average'
    END AS price_category
  FROM mytable as m
  JOIN category_stats cs
    ON m.category = cs.category
  ORDER BY m.category, m.price
