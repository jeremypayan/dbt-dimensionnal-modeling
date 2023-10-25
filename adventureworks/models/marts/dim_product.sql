WITH stg_product AS (
    SELECT productid, name AS productname, modifieddate, productmodelid, standardcost, productsubcategoryid, sellstartdate
    FROM {{ ref('product') }}
),

WITH stg_productcategory AS (
    SELECT productcategoryid, name AS productcategoryname, modifieddate
    FROM {{ ref('productcategory') }}
),

WITH stg_productsubcategory AS (
    SELECT productcategoryid, productsubcategoryid, name AS productsubcategoryname, modifieddate
    FROM {{ ref('productsubcategoryid') }}
)

SELECT {{ dbt_utils.generate_surrogate_key(['stg_product.productid']) }} as product_key,
       stg_product.productid,
       stg_product.productname,
       stg_product.color,
       productcategoryname,
       productsubcategoryname
FROM stg_product
LEFT JOIN stg_productecatgory
    ON stg_product.productmodelid = stg_productcategory.productcategoryid
LEFT JOIN stg_productsubcategory
    ON stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid