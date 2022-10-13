
WITH t AS (
    SELECT CONCAT(orderdate, '|', country) id 
    , orderdate
    , country
    , SUM(netamount) netamount
    , isValid 
    FROM {{ref('creditcard_fraud_raw')}} o
    GROUP BY orderdate, country, isValid
)

SELECT id
, orderdate
, country
, netamount 
FROM t 
WHERE NOT isValid 
ORDER BY orderdate