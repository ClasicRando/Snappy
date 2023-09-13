package org.snappy

import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp

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
        val value = when (input) {
            is Encode -> input
            is Byte -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setByte(parameterIndex, input)
            }
            is Short -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setShort(parameterIndex, input)
            }
            is Int -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setInt(parameterIndex, input)
            }
            is Long -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setLong(parameterIndex, input)
            }
            is Float -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setFloat(parameterIndex, input)
            }
            is Double -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setDouble(parameterIndex, input)
            }
            is BigDecimal -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setBigDecimal(parameterIndex, input)
            }
            is Date -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setDate(parameterIndex, input)
            }
            is Timestamp -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setTimestamp(parameterIndex, input)
            }
            is Time -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setTime(parameterIndex, input)
            }
            is String -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setString(parameterIndex, input)
            }
            is ByteArray -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setBytes(parameterIndex, input)
            }
            else -> Encode { preparedStatement, parameterIndex ->
                preparedStatement.setObject(parameterIndex, input)
            }
        }
    }
    /** Output SQL parameter. Only valid when a stored procedure call */
    class Out(val sqlType: Int): SqlParameter
}
