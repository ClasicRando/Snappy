package org.snappy.mssql.tvp

import com.microsoft.sqlserver.jdbc.SQLServerDataTable
import org.snappy.encode.Encode
import java.sql.PreparedStatement

abstract class AbstractTableType<R : ToTvpRow>(rows: Iterable<R>) : Encode {
    abstract val typeName: String
    abstract val columns: List<Pair<String, Int>>
    private val data = SQLServerDataTable().apply {
        tvpName = typeName
        for ((name, typeId) in columns) {
            addColumnMetadata(name, typeId)
        }
        for (row in rows) {
            addRow(*row.toTvpRow())
        }
    }

    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, data)
    }
}