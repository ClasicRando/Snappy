package org.snappy

import io.github.oshai.kotlinlogging.KLogger
import kotlinx.serialization.json.Json
import org.snappy.SnappyMapper.loadCache
import org.snappy.cache.DecoderCache
import org.snappy.cache.RowParserCache
import org.snappy.logging.logger
import kotlin.io.path.Path
import kotlin.io.path.exists
import kotlin.io.path.readText

/**
 * Main object storing cached data about an application using snappy. Although it's possible to
 * allow the caches to be loaded only when needed, it's recommended to call [loadCache] during
 * application startup to ensure the caches are populated before starting any operations that would
 * otherwise be blocked by cache initialization. This would be important for any web application
 * since without an initialized cache, the first request from a user to hit snappy method would be
 * blocked until the cache is initialized.
 */
object SnappyMapper {

    private val log: KLogger by logger()

    private val config: SnappyConfig by lazy {
        val path = Path(System.getenv("SNAPPY_CONFIG") ?: "snappy.json")
        val config = if (path.exists()) {
            val text = path.readText()
            val config = Json.decodeFromString<SnappyConfig>(text)
            log.atInfo {
                message = "Read json configuration"
                payload = mapOf("config" to config)
            }
            config
        } else {
            log.atInfo {
                message = "Using default configuration"
            }
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
     * Enables matching a result field name to a property/parameter, ignoring underscores. By
     * default, this is set as true
     */
    val allowUnderscoreMatch = config.allowUnderscoreMatch

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    fun loadCache() {
        decoderCache.loadCache()
        rowParserCache.loadCache()
    }

    init {
        loadCache()
    }
}
