
WITH t AS (
    SELECT o.orderdate
    , o.netamount
    , c.country
    , c.customerid
    , c.creditcard
    , {{Luhns('creditcard::int8')}} as isValid 
    FROM {{source('dellstore', 'orders')}} o
    JOIN {{source('dellstore', 'customers')}} c ON o.customerid = c.customerid
)

SELECT * FROM t 
ORDER BY orderdate