package org.snappy.mssql

import io.github.oshai.kotlinlogging.KotlinLogging
import org.snappy.decode.Decoder
import org.snappy.encode.Encode
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable
import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DateTime(datetime: LocalDateTime) : Encode {
    val value: LocalDateTime = datetime.let {
        val nanoAsStr = "%09d".format(it.nano)
        var newNano = nanoAsStr.slice(0..2).toInt()
        if (nanoAsStr[3].digitToInt() >= 5) {
            newNano++
        }
        when (val modulo = newNano % 10) {
            in 0..2 -> newNano -= modulo
            in 3..6 -> newNano -= (modulo - 3)
            in 7..9 -> newNano -= (modulo - 7)
        }
        it.withNano(newNano * 1_000_000)
    }
    private val valueAsStr: String = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSS")
        .format(value)

    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, valueAsStr, microsoft.sql.Types.DATETIME)
    }

    override fun toString(): String {
        return valueAsStr
    }

    override fun equals(other: Any?): Boolean {
        if (other is DateTime) {
            return other.value == this.value
        }
        return super.equals(other)
    }

    override fun hashCode(): Int {
        return 31 * value.hashCode() + valueAsStr.hashCode()
    }

    companion object : Decoder<DateTime> {
        override fun decode(row: SnappyRow, fieldName: String): DateTime? {
            return row.getObjectNullable<LocalDateTime>(fieldName)?.toDateTime()
        }
    }
}

fun LocalDateTime.toDateTime() = DateTime(this)
