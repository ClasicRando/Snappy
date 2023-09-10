package org.snappy

/**
 * Variants of SQL parameter types.
 *
 * Parameters in SQL statements depend on the [StatementType]. When the statement is
 * [Text][StatementType.Text] all parameters are of inputs only. When the statement is
 * [StoredProcedure][StatementType.StoredProcedure] parameters can either be `IN` (default) or
 * `OUT`. This means that for most cases, the parameters should be [SqlParameter.In] unless you are
 * calling a stored procedure with `OUT` parameters. This is why, when processing query parameters,
 * if the value is not [SqlParameter] they are implicitly wrapped as a [SqlParameter.In] and if a
 * [SqlParameter.Out] is found in a query that is not a
 * [StoredProcedure][StatementType.StoredProcedure] and exception is thrown.
 */
sealed interface SqlParameter {
    /**
     * Input SQL parameter. Default for text and stored procedure calls when parameter is not a
     * [SqlParameter] instance
     */
    class In(val value: Any?): SqlParameter
    /** Output SQL parameter. Only valid when a stored procedure call */
    class Out(val sqlType: Int): SqlParameter
}
