with composite_data as (
    select t.text_field, t.int_field, t.composite_field, t.int_array_field, t.composite_array_field
    from unnest(array[row(
         'This is a test',
         2314,
         row(
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
         )::simple_composite_test,
         array[1,5,6,4,5]::int[],
         array[
              row(
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
              )
         ]::simple_composite_test[]
     )]::complex_composite_test[]) t
)
select
    t1.text_field text_field_1, t1.int_field int_field_1, t1.composite_field composite_field_1,
    t1.int_array_field int_array_field_1, t1.composite_array_field composite_array_field_1,
    t2.text_field text_field_2, t2.int_field int_field_2, t2.composite_field composite_field_2,
    t2.int_array_field int_array_field_2, t2.composite_array_field composite_array_field_2
from composite_data t1
cross join unnest(array[?]::complex_composite_test[]) t2