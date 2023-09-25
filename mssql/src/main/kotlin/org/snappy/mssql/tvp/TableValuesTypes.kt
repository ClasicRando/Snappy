package org.snappy.mssql.tvp

import com.microsoft.sqlserver.jdbc.ISQLServerDataRecord
import com.microsoft.sqlserver.jdbc.SQLServerDataTable
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement
import org.snappy.encode.Encode
import java.sql.ResultSet

fun SQLServerDataTable.encode(typeName: String? = null): Encode {
    return Encode { preparedStatement, parameterIndex ->
        typeName?.let {
            this.tvpName = it
        }
        preparedStatement.setObject(parameterIndex, this)
    }
}

fun ResultSet.toTableValuedParameter(typeName: String): Encode {
    return Encode { preparedStatement, parameterIndex ->
        (preparedStatement as SQLServerPreparedStatement).setStructured(
            parameterIndex,
            typeName,
            this,
        )
    }
}

fun ISQLServerDataRecord.toTableValuedParameter(typeName: String): Encode {
    return Encode { preparedStatement, parameterIndex ->
        (preparedStatement as SQLServerPreparedStatement).setStructured(
            parameterIndex,
            typeName,
            this,
        )
    }
}
