create or alter procedure tvp_test_procedure
    @rows tvp_test readonly
as
begin
    select *
    from @rows;
end;
