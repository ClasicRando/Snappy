package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.rowparse.SnappyRow
import org.snappy.rowparse.getObjectNullable

/**
 * [Decoder] for types that would be fetched from a [SnappyRow] as a [PGobject]. That [PGobject]
 * would then be decoded into the type [T] using the [decodePgObject] method, implemented by the
 * concrete classes.
 *
 * For classes that want to implement this interface to allow for decoding of a composite or enum
 * postgres type, you can use the ksp annotation, [PgType], to code generate your decoder for you.
 */
interface PgObjectDecoder<T : Any> : Decoder<T> {
    /**
     * Decode the provided [pgObject] into the desired type [T], possibly returning null when the
     * [PGobject.value] is null.
     */
    fun decodePgObject(pgObject: PGobject): T?

    override fun decodeNullable(row: SnappyRow, fieldName: String): T? {
        return row.getObjectNullable<PGobject>(fieldName)?.let {
            decodePgObject(it)
        }
    }
}
