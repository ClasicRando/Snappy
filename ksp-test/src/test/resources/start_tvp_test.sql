if TYPE_ID('tvp_test') is null
begin
    create type tvp_test as table (
        bool_field bit,
        smallint_field smallint,
        int_field int,
        bigint_field bigint,
        real_field float(24),
        double_field float(53),
        text_field text,
        numeric_field decimal(9,5),
        date_field date,
        datetime_field datetime,
        smalldatetime_field smalldatetime,
        datetimeoffset_field datetimeoffset,
        time_field time
    );
end;
