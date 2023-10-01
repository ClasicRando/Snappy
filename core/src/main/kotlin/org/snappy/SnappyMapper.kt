package org.snappy

import kotlinx.serialization.json.Json
import org.snappy.cache.DecoderCache
import org.snappy.cache.RowParserCache
import java.io.File

/**
 * Main object storing cached data about an application using snappy. Although it's possible to
 * allow the caches to be loaded only when needed, it's recommended to call [loadCache] during
 * application startup to ensure the caches are populated before starting any operations that would
 * otherwise be blocked by cache initialization. This would be important for any web application
 * since without an initialized cache, the first request from a user to hit snappy method would be
 * blocked until the cache is initialized.
 */
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

    /** Cache of [org.snappy.rowparse.RowParser] classes available within the application */
    val rowParserCache = RowParserCache(config)

    /** Cache of [org.snappy.decode.Decoder] classes available within the application */
    val decoderCache = DecoderCache(config)

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    fun loadCache() {
        decoderCache.loadCache()
        rowParserCache.loadCache()
    }
}
