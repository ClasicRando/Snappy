package org.snappy.mssql.tvp

class TvpTest(rows: Iterable<TvpTestRow>) : AbstractTableType<TvpTestRow>(rows) {
    override val columns: List<Pair<String, Int>> = listOf(
        "bool_field" to java.sql.Types.BIT,
        "smallint_field" to java.sql.Types.SMALLINT,
        "int_field" to java.sql.Types.INTEGER,
        "bigint_field" to java.sql.Types.BIGINT,
        "real_field" to java.sql.Types.REAL,
        "double_field" to java.sql.Types.DOUBLE,
        "text_field" to java.sql.Types.VARCHAR,
        "numeric_field" to java.sql.Types.NUMERIC,
        "date_field" to java.sql.Types.DATE,
        "timestamp_field" to java.sql.Types.TIMESTAMP,
        "smalldatetime_field" to microsoft.sql.Types.SMALLDATETIME,
        "datetimeoffset_field" to microsoft.sql.Types.DATETIMEOFFSET,
        "time_field" to java.sql.Types.TIME,
    )
    override val typeName: String = "tvp_test"
}
