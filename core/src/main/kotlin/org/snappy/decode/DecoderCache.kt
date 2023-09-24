@file:Suppress("UNCHECKED_CAST")
package org.snappy.decode

import io.github.classgraph.ClassGraph
import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyConfig
import org.snappy.TypeArgumentMismatch
import org.snappy.logging.logger
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.withNullability
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf
import kotlin.system.measureTimeMillis

class DecoderCache internal constructor(private val config: SnappyConfig) {
    /** [KLogger][io.github.oshai.kotlinlogging.KLogger] for this cache instance */
    private val log by logger()
    /** Internally used flag to indicate if the cache has been loaded */
    internal var cacheLoaded: Boolean = false
        private set
    /** Map of an output type linked to a [Decoder] */
    internal val cache: ConcurrentHashMap<KType, Decoder<*>> by lazy { initCache() }

    private fun initCache(): ConcurrentHashMap<KType, Decoder<*>> {
        val start = System.currentTimeMillis()
        val cache = ConcurrentHashMap<KType, Decoder<*>>()
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
            "Decoder cache has been initialized. Took %.2f seconds".format((end - start)/1000.0)
        }
        return cache
    }

    internal val defaultDecoder = Decoder { it }

    /**
     * Get a [Decoder] for the provided type [rowType]. Checks the [cache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    @PublishedApi
    internal fun getOrDefault(rowType: KType): Decoder<*> {
        return cache[rowType] ?: defaultDecoder
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [cache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrDefault(): Decoder<*> {
        return getOrDefault(typeOf<T>())
    }

    /**
     * Get a [Decoder] for the provided type [rowType]. Checks the [cache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    fun getOrNull(rowType: KType): Decoder<*>? {
        return cache[rowType]
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [cache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): Decoder<T>? {
        return getOrNull(typeOf<T>()) as Decoder<T>?
    }

    /** Add or replace an existing parser with a new [parser] for the [rowType] specified */
    @PublishedApi
    internal fun <T : Any> insertOrReplace(rowType: KType, parser: Decoder<T>) {
        cache[rowType.withNullability(false)] = parser
    }

    /** Add or replace an existing parser with a new [parser] for the [T] specified */
    inline fun <reified T : Any> insertOrReplace(parser: Decoder<T>) {
        insertOrReplace(typeOf<T>(), parser)
    }

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    internal fun loadCache() {
        cache
    }

    private val decoderInterfaceKClass = Decoder::class

    /**
     * Yield pairs of a [KType] and [Decoder] to initialize the cache with classes implementing
     * [Decoder]
     */
    private fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesImplementing(Decoder::class.java)) {
            if (classInfo.isAbstract) {
                continue
            }
            val cls = classInfo.loadClass()
            val kClass = cls.kotlin
            yield(parseDecoder(kClass as KClass<Decoder<*>>))
        }
    }

    /**
     * Process and return a [Decoder] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     */
    private fun parseDecoder(kClass: KClass<Decoder<*>>): Pair<KType, Decoder<*>> {
        val valueClassTypeArgument = kClass.supertypes
            .first { it.jvmErasure.isSubclassOf(decoderInterfaceKClass) }
            .arguments
            .first()
            .type!!
        val valueClass = valueClassTypeArgument.jvmErasure
        if (valueClass.typeParameters.isNotEmpty()
            && valueClassTypeArgument.arguments.size != valueClass.typeParameters.size) {
            throw TypeArgumentMismatch(valueClass, valueClassTypeArgument)
        }
        val decoder = try {
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
            nullable = false,
            arguments = if (valueClass.typeParameters.isNotEmpty()) {
                valueClassTypeArgument.arguments
            } else listOf(),
        ) to decoder
    }
}