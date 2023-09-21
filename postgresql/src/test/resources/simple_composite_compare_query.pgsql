select
    t1.bool_field bool_field1, t1.smallint_field smallint_field1, t1.int_field int_field1,
    t1.bigint_field bigint_field1, t1.real_field real_field1, t1.double_field double_field1,
    t1.text_field text_field1, t1.numeric_field numeric_field1, t1.date_field date_field1,
    t1.timestamp_field timestamp_field1, t1.timestamptz_field timestamptz_field1,
    t1.time_field time_field1, t1.timetz_field timetz_field1,
    t2.bool_field bool_field2, t2.smallint_field smallint_field2, t2.int_field int_field2,
    t2.bigint_field bigint_field2, t2.real_field real_field2, t2.double_field double_field2,
    t2.text_field text_field2, t2.numeric_field numeric_field2, t2.date_field date_field2,
    t2.timestamp_field timestamp_field2, t2.timestamptz_field timestamptz_field2,
    t2.time_field time_field2, t2.timetz_field timetz_field2
from unnest(array[row(
    true,
    1,
    1,
    1,
    1.0,
    1.0,
    'Test',
    1,
    '2023-01-01',
    '2023-01-01 00:00:00',
    '2023-01-01 00:00:00+0'::timestamp with time zone,
    '00:00:00',
    '00:00:00-04'
)::simple_composite_test]) t1
cross join unnest(array[?]::simple_composite_test[]) t2