package org.snappy.mssql

import org.snappy.decode.Decoder
import org.snappy.encode.Encode
import org.snappy.rowparse.SnappyRow
import java.sql.PreparedStatement
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Wrapper of a [LocalDateTime] used to represent the smalldatetime type in SQL Server. The data
 * type stores a maximum precision of 1 minute for the values so the second and nano value of the
 * underling [LocalDateTime] are removed when creating a [SmallDateTime]. The type is able to be
 * encoded into a [PreparedStatement] and decoded from a [SnappyRow].
 */
class SmallDateTime(localDateTime: LocalDateTime) : Encode {
    /**
     * Underling [LocalDateTime] that backs the [SmallDateTime]. Strips the second and nano
     * precision.
     */
    val value: LocalDateTime = localDateTime.withSecond(0).withNano(0)
    /** String representation of the value as a regular datetime value */
    private val valueAsStr: String = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:00")
        .format(value)

    /**
     * Get the value in a format the bcp client can understand. Special case since smalldatetime
     * type has a length of 16 but the [valueAsStr] representation contains too many bytes.
     */
    fun bulkCopyString(): String = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm")
        .format(value)

    override fun encode(preparedStatement: PreparedStatement, parameterIndex: Int) {
        preparedStatement.setObject(parameterIndex, valueAsStr, microsoft.sql.Types.SMALLDATETIME)
    }

    override fun toString(): String {
        return valueAsStr
    }

    override fun equals(other: Any?): Boolean {
        if (other is SmallDateTime) {
            return other.value == this.value
        }
        return super.equals(other)
    }

    override fun hashCode(): Int {
        return 31 * value.hashCode() + valueAsStr.hashCode()
    }

    companion object : Decoder<SmallDateTime> {
        override fun decodeNullable(row: SnappyRow, fieldName: String): SmallDateTime? {
            return row.getLocalDateTimeNullable(fieldName)?.toSmallDateTime()
        }
    }
}

/** Convert a [LocalDateTime] to a [SmallDateTime]. Creates a new [LocalDateTime] from this value */
fun LocalDateTime.toSmallDateTime() = SmallDateTime(this)
