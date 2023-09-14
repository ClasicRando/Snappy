package org.snappy.decode

fun interface Decoder<T> {
    fun decode(value: Any?): T
}
