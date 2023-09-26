package org.snappy.mssql

import org.snappy.decode.Decoder
import org.snappy.encode.Encode
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable
import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SmallDateTime(localDateTime: LocalDateTime) : Encode {
    val value: LocalDateTime = localDateTime.withSecond(0)
    private val valueAsStr = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:00")
        .format(value)

    override fun toString(): String {
        return valueAsStr
    }

    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, valueAsStr, microsoft.sql.Types.SMALLDATETIME)
    }

    override fun equals(other: Any?): Boolean {
        if (other is SmallDateTime) {
            return other.value == this.value
        }
        return super.equals(other)
    }

    override fun hashCode(): Int {
        var result = value?.hashCode() ?: 0
        result = 31 * result + (valueAsStr?.hashCode() ?: 0)
        return result
    }

    companion object : Decoder<SmallDateTime> {
        override fun decode(row: SnappyRow, fieldName: String): SmallDateTime? {
            return row.getObjectNullable<LocalDateTime>(fieldName)?.toSmallDateTime()
        }
    }
}

fun LocalDateTime.toSmallDateTime() = SmallDateTime(this)
