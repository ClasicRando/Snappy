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
            SnappyConfig(packages = mutableListOf())
        }
        config.packages.add("org.snappy")
        config
    }

    val rowParserCache = RowParserCache(config)

    val decoderCache = DecoderCache(config)

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    fun loadCache() {
        rowParserCache.loadCache()
        decoderCache.loadCache()
    }

    fun temp() {
        println(decoderCache.cache.entries.joinToString("\n") { "${it.key} -> ${it.value}" })
    }

    init { loadCache() }
}