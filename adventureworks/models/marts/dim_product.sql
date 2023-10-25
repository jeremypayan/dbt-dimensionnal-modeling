WITH stg_product AS (
    SELECT productid, name AS productname, modifieddate, productmodelid, standardcost, productsubcategoryid, sellstartdate, color
    FROM {{ ref('product') }}
),

stg_productcategory AS (
    SELECT productcategoryid, name AS productcategoryname, modifieddate
    FROM {{ ref('productcategory') }}
),

stg_productsubcategory AS (
    SELECT productcategoryid, productsubcategoryid, name AS productsubcategoryname, modifieddate
    FROM {{ ref('productsubcategory') }}
)

SELECT {{ dbt_utils.generate_surrogate_key(['stg_product.productid']) }} as product_key,
       stg_product.productid,
       stg_product.productname AS product_name,
       stg_product.color,
       stg_productcategory.productcategoryname,
       stg_productsubcategory.productsubcategoryname
FROM stg_product
LEFT JOIN stg_productcategory
    ON stg_product.productmodelid = stg_productcategory.productcategoryid
LEFT JOIN stg_productsubcategory
    ON stg_product.productsubcategoryid = stg_productsubcategory.productsubcategoryid