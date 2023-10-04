package org.snappy.command

import org.snappy.statement.SqlParameter
import org.snappy.statement.StatementType

interface Command {
    val sql: String
    val statementType: StatementType
    val timeout: UInt?
    val commandParameters: List<SqlParameter>
    fun parameterCount(): Int
}
