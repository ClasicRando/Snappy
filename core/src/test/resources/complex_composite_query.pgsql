select row(
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
)::complex_composite_test