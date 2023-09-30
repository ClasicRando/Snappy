@file:Suppress("UNCHECKED_CAST")

package org.snappy.rowparse

import io.github.classgraph.ClassGraph
import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyConfig
import org.snappy.logging.logger
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

/**
 * Cache of [RowParser] implementation for a desired row type. Backed by a [ConcurrentHashMap] to
 * allow for multiple threads to access the cache at once.
 */
class RowParserCache internal constructor(private val config: SnappyConfig) {
    /** [KLogger][io.github.oshai.kotlinlogging.KLogger] for this cache instance */
    private val log by logger()
    /** Internally used flag to indicate if the cache has been loaded */
    internal var cacheLoaded: Boolean = false
        private set
    /** Map of an output type linked to a [RowParser] */
    private val rowParserCache: HashMap<KType, RowParser<*>> by lazy { initCache() }

    private fun initCache(): HashMap<KType, RowParser<*>> {
        val start = System.currentTimeMillis()
        val cache = HashMap<KType, RowParser<*>>()
        val packages = config.packages.toTypedArray()
        ClassGraph().enableAllInfo().acceptPackages(*packages).scan().use { result ->
            for((type, decoder) in processAllAutoCacheClasses(result)) {
                if (cache.contains(type)) {
                    log.warn {
                        "Multiple decoders exist for type '$type'. The latest one seen is kept"
                    }
                }
                cache[type] = decoder
            }
        }
        cacheLoaded = true
        val end = System.currentTimeMillis()
        log.info {
            "RowParser cache has been initialized. Took %.2f seconds".format((end - start)/1000.0)
        }
        return cache
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [rowType] and returns that new parser.
     */
    @PublishedApi
    internal fun <T : Any> getOrThrow(rowType: KType): RowParser<T> {
        val cachedResult = rowParserCache[rowType]
            ?: error("No RowParser cached for '$rowType'")
        return cachedResult as RowParser<T>
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [T] and returns that new parser.
     */
    inline fun <reified T : Any> getOrThrow(): RowParser<T> {
        return getOrThrow(typeOf<T>())
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    @PublishedApi
    internal fun <T : Any> getOrNull(rowType: KType): RowParser<T>? {
        return rowParserCache[rowType] as? RowParser<T>
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): RowParser<T>? {
        return getOrNull(typeOf<T>())
    }

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    internal fun loadCache() {
        rowParserCache
    }

    private val rowParserInterfaceKClass = RowParser::class

    /**
     * Yield pairs of a [KType] and [RowParser] to initialize the cache with classes that extend
     * [RowParser]
     */
    private fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesImplementing(RowParser::class.java)) {
            if (classInfo.isAbstract) {
                continue
            }
            if (classInfo.name == "org.snappy.rowparse.DataClassParser"
                || classInfo.name == "org.snappy.rowparse.DefaultRowParser") {
                continue
            }
            val cls = classInfo.loadClass()
            val kClass = cls.kotlin
            yield(createRowParserClass(kClass as KClass<RowParser<*>>))
        }
    }

    /**
     * Process and return a [RowParser] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     */
    private fun createRowParserClass(kClass: KClass<RowParser<*>>): Pair<KType, RowParser<*>> {
        val valueClassTypeArgument = kClass.supertypes
            .first { it.jvmErasure.isSubclassOf(rowParserInterfaceKClass) }
            .arguments
            .first()
            .type!!
        val valueClass = valueClassTypeArgument.jvmErasure
        val rowParser = try {
            if (kClass.isCompanion) {
                kClass.objectInstance
                    ?: throw IllegalStateException("Companion object must have object instance")
            } else {
                kClass.createInstance()
            }
        } catch (_: IllegalArgumentException) {
            throw NoDefaultConstructor(valueClass)
        }
        return valueClass.createType(
            arguments = valueClassTypeArgument.arguments
        ) to rowParser
    }
}
