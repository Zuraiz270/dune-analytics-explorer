
WITH params AS (
  SELECT 
    '{{symbol}}' AS symbol,  -- Placeholder for the symbol
    '{{period}}' AS period   -- Placeholder for the period ('day', 'week', 'month')
),

uniswap_filtered_trades AS (
  SELECT
    t.block_time,
    -- Identify if the trade involves our symbol
    CASE
      WHEN t.token_sold_symbol = params.symbol
           OR t.token_bought_symbol = params.symbol
      THEN params.symbol
      ELSE NULL
    END AS symbol,

    t.amount_usd,

    -- Use fee tier from c.fee and convert to percentage
    c.fee / CAST(1e6 AS DOUBLE) AS fee_tier,

    -- Calculate fees collected directly in USD
    t.amount_usd * (c.fee / CAST(1e6 AS DOUBLE)) AS fees_usd

  FROM dex.trades AS t
  LEFT JOIN uniswap_v3_ethereum.Factory_call_createPool AS c
    ON t.project_contract_address = c.output_pool
  JOIN params
    ON (t.token_sold_symbol = params.symbol
        OR t.token_bought_symbol = params.symbol)
  WHERE t.blockchain = 'ethereum'
    AND t.project = 'uniswap'
    AND t.version = '3'
    AND t.block_time >= TRY_CAST('2021-01-01 00:00:00' AS TIMESTAMP)
    AND t.block_time <= CURRENT_TIMESTAMP
),

aggregated_data AS (
  SELECT
    CASE
      WHEN params.period = 'day'   THEN DATE_TRUNC('day',   uf.block_time)
      WHEN params.period = 'week'  THEN DATE_TRUNC('week',  uf.block_time)
      WHEN params.period = 'month' THEN DATE_TRUNC('month', uf.block_time)
      WHEN params.period = 'year'  THEN DATE_TRUNC('year',  uf.block_time)
      ELSE DATE_TRUNC('day', uf.block_time)
    END AS period,
    uf.symbol,
    SUM(uf.amount_usd) AS volume_usd,
    SUM(uf.fees_usd) AS fees_usd -- Aggregate precomputed fees
  FROM uniswap_filtered_trades uf
  JOIN params ON TRUE
  WHERE uf.symbol IS NOT NULL
  GROUP BY 1, uf.symbol
)

SELECT
  period,
  symbol,
  volume_usd,
  fees_usd
FROM aggregated_data
ORDER BY period DESC;
