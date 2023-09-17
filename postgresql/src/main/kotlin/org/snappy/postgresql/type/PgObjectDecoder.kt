package org.snappy.postgresql.type

import org.postgresql.util.PGobject
import org.snappy.decode.Decoder
import org.snappy.decodeError
import kotlin.reflect.KClass

interface PgObjectDecoder<T : Any> : Decoder<T> {
    val decodeClass: KClass<T>
    fun decodePgObject(pgObject: PGobject): T
    override fun decode(value: Any): T {
        if (value is PGobject) {
            return decodePgObject(value)
        }
        decodeError(decodeClass, value)
    }
}
