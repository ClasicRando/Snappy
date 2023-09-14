package org.snappy

import org.snappy.encode.Encode
import org.snappy.encode.toEncodable

/**
 * Variants of SQL parameter types.
 *
 * Parameters in SQL statements depend on the [StatementType]. When the statement is
 * [Text][StatementType.Text] all parameters are of inputs only. When the statement is
 * [StoredProcedure][StatementType.StoredProcedure] parameters can either be `IN` (default) or
 * `OUT`. This means that for most cases, the parameters should be [SqlParameter.In] unless you are
 * calling a stored procedure with `OUT` parameters.
 */
sealed interface SqlParameter {
    /**
     * Input SQL parameter. Default for text and stored procedure calls when parameter is not a
     * [SqlParameter] instance. Instances are created by passing any input value which is then
     * type checked to convert into the appropriate [Encode] method. Note: even though this class
     * can take any input type, when encoding into the [java.sql.PreparedStatement], the encoding
     * might fail at runtime since the type cannot actually be added to a statement.
     */
    class In(input: Any?): SqlParameter {
        val value = toEncodable(input)
    }
    /** Output SQL parameter. Only valid when a stored procedure call */
    class Out(val sqlType: Int): SqlParameter
}
