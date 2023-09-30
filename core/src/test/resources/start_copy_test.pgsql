create table if not exists copy_test (
    bool_field boolean,
    smallint_field smallint,
    int_field int,
    bigint_field bigint,
    real_field real,
    double_field double precision,
    text_field text,
    numeric_field numeric,
    date_field date,
    timestamp_field timestamp,
    timestamptz_field timestamp with time zone,
    time_field time,
    timetz_field time with time zone
);
