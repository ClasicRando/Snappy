package org.snappy.data

import org.snappy.SnappyConfig
import org.snappy.decode.Decoder

class CacheMissDecoderClass : Decoder<SnappyConfig> {
    override fun decode(value: Any): SnappyConfig {
        return SnappyConfig(mutableListOf())
    }
}
