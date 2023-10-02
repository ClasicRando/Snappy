package org.snappy.mssql.tvp

import com.microsoft.sqlserver.jdbc.SQLServerDataTable
import org.snappy.encode.Encode
import java.sql.PreparedStatement

/**
 * Base class for custom Table Valued Parameter types. Stores an [Iterable] of [ToTvpRow] items that
 * are packed into a [SQLServerDataTable] when encoding. To create custom table types within your
 * code, create a data class that implements [ToTvpRow], then extend this class and provide the
 * [typeName] and [columns] details. Any instances of that derived class would then have the ability
 * to be encoded into a [PreparedStatement].
 */
abstract class AbstractTableType<R : ToTvpRow>(private val rows: Iterable<R>) : Encode {
    abstract val typeName: String
    abstract val columns: List<Pair<String, Int>>
    private val data = SQLServerDataTable()

    private fun initData() {
        data.apply {
            tvpName = typeName
            for ((name, typeId) in columns) {
                addColumnMetadata(name, typeId)
            }
            var hasCheckedSize = false
            for (row in rows) {
                val items = row.toTvpRow()
                if (!hasCheckedSize) {
                    check(columns.size == items.size)
                    hasCheckedSize = true
                }
                addRow(*items)
            }
        }
    }

    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        initData()
        preparedStatement.setObject(parameterIndex, data)
    }
}
