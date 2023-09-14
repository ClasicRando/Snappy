package org.snappy

import kotlinx.serialization.json.Json
import org.snappy.decode.DecoderCache
import org.snappy.rowparse.RowParserCache
import java.io.File

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

    fun loadCache() {
        rowParserCache.loadCache()
        decoderCache.loadCache()
    }
}