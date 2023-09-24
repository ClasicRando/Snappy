do $$
begin
    if not exists(select null from pg_type where typname = 'simple_composite_test') then
        create type simple_composite_test as (
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
    end if;
    if not exists(select null from pg_type where typname = 'complex_composite_test') then
        create type complex_composite_test as (
            text_field text,
            int_field int,
            composite_field simple_composite_test,
            int_array_field int[],
            composite_array_field simple_composite_test[]
        );
    end if;
end;
$$;