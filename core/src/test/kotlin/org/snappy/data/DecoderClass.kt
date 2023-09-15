package org.snappy.data

import org.snappy.decode.Decoder

class DecoderClass :Decoder<RowClass> {
    override fun decode(value: Any): RowClass {
        return RowClass("")
    }
}
