CREATE OR REFRESH MATERIALIZED VIEW bronze_pos_transactions
COMMENT "Raw point-of-sale transactions persisted from samples.bakehouse.sales_transactions"
AS
SELECT
  CAST(transactionID AS STRING) AS raw_transaction_id,
  CAST(dateTime AS TIMESTAMP) AS transaction_timestamp,
  product AS product_sold,
  CAST(quantity AS INT) AS quantity,
  CAST(unitPrice AS DECIMAL(10,2)) AS unit_price,
  CAST(totalPrice AS DECIMAL(12,2)) AS total_price,
  CAST(customerID AS STRING) AS customer_id,
  CAST(franchiseID AS STRING) AS store_id,
  paymentMethod AS payment_method,
  CAST(cardNumber AS STRING) AS card_number,
  current_timestamp() AS _ingested_at,
  'samples.bakehouse.sales_transactions' AS _source_table
FROM samples.bakehouse.sales_transactions;
