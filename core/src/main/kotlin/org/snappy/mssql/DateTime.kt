package org.snappy.mssql

import org.snappy.decode.Decoder
import org.snappy.encode.Encode
import org.snappy.rowparse.SnappyRow
import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Wrapper of a [LocalDateTime] used to represent the datetime type in SQL Server. The data type
 * stores a maximum precision of 0.00333 seconds for the values so the second and nano value of the
 * underling [LocalDateTime] are altered when creating a [DateTime]. The type is able to be encoded
 * into a [PreparedStatement] and decoded from a [SnappyRow].
 */
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
        override fun decodeNullable(row: SnappyRow, fieldName: String): DateTime? {
            return row.getLocalDateTimeNullable(fieldName)?.toDateTime()
        }
    }
}

/** Convert a [LocalDateTime] to a [DateTime]. Creates a new [LocalDateTime] from this value */
fun LocalDateTime.toDateTime() = DateTime(this)
