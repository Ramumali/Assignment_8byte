-- workspace/sql/create_table.sql
CREATE TABLE IF NOT EXISTS stocks_time_series (
  symbol TEXT NOT NULL,
  ts TIMESTAMP WITH TIME ZONE NOT NULL,
  open NUMERIC,
  high NUMERIC,
  low NUMERIC,
  close NUMERIC,
  adjusted_close NUMERIC,
  volume BIGINT,
  PRIMARY KEY (symbol, ts)
);