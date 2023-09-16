package org.snappy.encode

import java.sql.PreparedStatement

/**
 * Interface for denoting a type is encodable into a [PreparedStatement] parameter. This allows the
 * type to be implicitly wrapped into a [SqlParameter.In][org.snappy.statement.SqlParameter.In] and
 * encoded using the logic found within [encode]. This interface is also a functional interface to
 * allow for easy one off implementations of [Encode] without specifying an anonymous object
 * implementing the interface.
 */
fun interface Encode {
    /**
     * Convert the current object into a [PreparedStatement] parameter. This function should only
     * set a parameter into the [preparedStatement] at the specified [parameterIndex] after
     * transforming the data into the required format. The implementor should NEVER alter the
     * statement in any other way (e.g. closing the statement).
     */
    fun encode(preparedStatement: PreparedStatement, parameterIndex: Int)
}
