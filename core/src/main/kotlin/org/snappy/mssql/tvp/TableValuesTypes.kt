package org.snappy.mssql.tvp

import com.microsoft.sqlserver.jdbc.ISQLServerDataRecord
import com.microsoft.sqlserver.jdbc.SQLServerDataTable
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement
import org.snappy.encode.Encode
import java.sql.ResultSet

/**
 * Allows for encoding a [SQLServerDataTable] as a Table Valued Parameter. If [typeName] is
 * specified, the [SQLServerDataTable.tvpName] property is updated, otherwise it's assumed that the
 * property was already set
 */
fun SQLServerDataTable.encode(typeName: String? = null): Encode {
    return Encode { preparedStatement, parameterIndex ->
        typeName?.let {
            this.tvpName = it
        }
        preparedStatement.setObject(parameterIndex, this)
    }
}

/**
 * Allows for encoding a [ResultSet] as a Table Valued Parameter. Calls
 * [SQLServerPreparedStatement.setStructured] method with the [typeName] provided.
 */
fun ResultSet.toTableValuedParameter(typeName: String): Encode {
    return Encode { preparedStatement, parameterIndex ->
        (preparedStatement as SQLServerPreparedStatement).setStructured(
            parameterIndex,
            typeName,
            this,
        )
    }
}

/**
 * Allows for encoding an [ISQLServerDataRecord] as a Table Valued Parameter. Calls
 * [SQLServerPreparedStatement.setStructured] method with the [typeName] provided.
 */
fun ISQLServerDataRecord.toTableValuedParameter(typeName: String): Encode {
    return Encode { preparedStatement, parameterIndex ->
        (preparedStatement as SQLServerPreparedStatement).setStructured(
            parameterIndex,
            typeName,
            this,
        )
    }
}
