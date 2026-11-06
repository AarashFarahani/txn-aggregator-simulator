CREATE TABLE IF NOT EXISTS transaction_aggregates (
  id BIGSERIAL PRIMARY KEY,
  aggregation_id VARCHAR(160) NOT NULL UNIQUE,
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  aggregation_type VARCHAR(50) NOT NULL,
  dimension_type VARCHAR(50) NOT NULL,
  dimension_value VARCHAR(120) NOT NULL,
  transaction_count BIGINT NOT NULL,
  total_amount NUMERIC(20,2) NOT NULL,
  average_amount NUMERIC(20,2) NOT NULL,
  min_amount NUMERIC(20,2) NOT NULL,
  max_amount NUMERIC(20,2) NOT NULL,
  approved_count BIGINT NOT NULL,
  declined_count BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_agg_window ON transaction_aggregates(window_start);
CREATE INDEX IF NOT EXISTS idx_agg_dimension ON transaction_aggregates(dimension_type,dimension_value);
