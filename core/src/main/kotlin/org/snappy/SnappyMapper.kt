package org.snappy

import kotlinx.serialization.json.Json
import org.snappy.decode.AnyDecoder
import org.snappy.decode.BigDecimalDecoder
import org.snappy.decode.BooleanDecoder
import org.snappy.decode.ByteArrayDecoder
import org.snappy.decode.ByteDecoder
import org.snappy.decode.DateDecoder
import org.snappy.decode.DecoderCache
import org.snappy.decode.DoubleDecoder
import org.snappy.decode.FloatDecoder
import org.snappy.decode.InstantDecoder
import org.snappy.decode.IntDecoder
import org.snappy.decode.LongDecoder
import org.snappy.decode.ShortDecoder
import org.snappy.decode.StringDecoder
import org.snappy.decode.TimeDecoder
import org.snappy.decode.TimestampDecoder
import org.snappy.rowparse.RowParserCache
import java.io.File
import java.math.BigDecimal
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant

object SnappyMapper {

    private val config: SnappyConfig by lazy {
        val file = File("snappy.json")
        val config = if (file.exists()) {
            val text = file.readText()
            Json.decodeFromString<SnappyConfig>(text)
        } else {
            SnappyConfig(basePackages = mutableListOf())
        }
        config.basePackages.add("org.snappy")
        config
    }

    @PublishedApi
    internal val rowParserCache = RowParserCache(config)

    @PublishedApi
    internal val decoderCache = DecoderCache(config)

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    fun loadCache() {
        rowParserCache.loadCache()
        decoderCache.loadCache()
        with(decoderCache) {
            insertOrReplace<Any>(AnyDecoder())
            insertOrReplace<Boolean>(BooleanDecoder())
            insertOrReplace<Byte>(ByteDecoder())
            insertOrReplace<Short>(ShortDecoder())
            insertOrReplace<Int>(IntDecoder())
            insertOrReplace<Long>(LongDecoder())
            insertOrReplace<Float>(FloatDecoder())
            insertOrReplace<Double>(DoubleDecoder())
            insertOrReplace<BigDecimal>(BigDecimalDecoder())
            insertOrReplace<Date>(DateDecoder())
            insertOrReplace<Timestamp>(TimestampDecoder())
            insertOrReplace<Time>(TimeDecoder())
            insertOrReplace<String>(StringDecoder())
            insertOrReplace<ByteArray>(ByteArrayDecoder())
            insertOrReplace<Instant>(InstantDecoder())
        }
    }

    init { loadCache() }
}