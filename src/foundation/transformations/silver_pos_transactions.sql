CREATE OR REFRESH MATERIALIZED VIEW silver_pos_transactions
COMMENT "Deduplicated, validated transactions with referential integrity checks"
AS
WITH deduped AS (
  SELECT
    raw_transaction_id AS transaction_id,
    transaction_timestamp,
    product_sold,
    quantity,
    unit_price,
    COALESCE(total_price, CAST(quantity * unit_price AS DECIMAL(12,2))) AS total_price,
    customer_id,
    store_id,
    row_number() OVER (
      PARTITION BY raw_transaction_id
      ORDER BY transaction_timestamp DESC
    ) AS rn
  FROM bronze_pos_transactions
  WHERE raw_transaction_id IS NOT NULL
),
valid_fk AS (
  SELECT d.*
  FROM deduped d
  INNER JOIN silver_customer_profiles c
    ON d.customer_id = c.customer_id
  INNER JOIN silver_franchise_ops f
    ON d.store_id = f.store_id
  WHERE d.rn = 1
    AND d.quantity > 0
    AND d.unit_price > 0
)
SELECT * EXCEPT (rn)
FROM valid_fk;
