CREATE TABLE IF NOT EXISTS currency (
    id UUID,
    date Date,
    usd Float64,
    euro Float64,
    created DateTime,
    updated Nullable(DateTime)
) ENGINE = MergeTree()
ORDER BY (date);