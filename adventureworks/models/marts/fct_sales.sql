WITH stg_salesorderheader AS (
    SELECT salesorderheader.salesorderid,
           salesorderheader.shiptoaddressid, 
           salesorderheader.territoryid, 
           salesorderheader.status AS orderstatus, 
           salesorderheader.creditcardid,
           CAST(salesorderheader.orderdate AS date) as orderdate, 
           salesorderheader.totaldue, 
           salesorderheader.customerid
    FROM {{ ref('salesorderheader')}}
), 
    stg_salesorderdetail AS (
    SELECT salesorderdetail.salesorderid,
           salesorderdetail.salesorderdetailid,
           salesorderdetail.productid,
           salesorderdetail.orderqty,
           salesorderdetail.unitprice,
           salesorderdetail.unitprice * orderqty as revenue
    FROM {{ref('salesorderdetail')}}
    )

SELECT  {{ dbt_utils.generate_surrogate_key(['stg_salesorderdetail.salesorderid', 'salesorderdetailid']) }} as sales_key,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_key,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as creditcard_key,
        {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} as ship_address_key,
        {{ dbt_utils.generate_surrogate_key(['orderstatus']) }} as order_status_key,
        {{ dbt_utils.generate_surrogate_key(['orderdate']) }} as order_date_key,
        stg_salesorderheader.salesorderid,
        stg_salesorderdetail.salesorderdetailid,
        stg_salesorderdetail.unitprice,
        stg_salesorderdetail.orderqty,
        stg_salesorderdetail.revenue
FROM stg_salesorderheader
INNER JOIN stg_salesorderdetail ON stg_salesorderheader.salesorderid = stg_salesorderdetail.salesorderid