@file:Suppress("UNCHECKED_CAST")
package org.snappy.decode

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
import org.snappy.CannotFindDecodeValueType
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyConfig
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

class DecoderCache internal constructor(private val config: SnappyConfig) {
    /** Internally used flag to indicate if the cache has been loaded */
    internal var cacheLoaded: Boolean = false
        private set
    /** Map of an output type linked to a [Decoder] */
    private val decoderCache: ConcurrentHashMap<KType, Decoder<*>> by lazy {
        val cache = ConcurrentHashMap<KType, Decoder<*>>()
        val packages = config.basePackages.toTypedArray()
        ClassGraph().enableAllInfo().acceptPackages(*packages).scan().use { result ->
            processAllAutoCacheClasses(result).toMap(cache)
        }
        cacheLoaded = true
        cache
    }

    internal val defaultDecoder = Decoder { it }

    /**
     * Get a [Decoder] for the provided type [rowType]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    @PublishedApi
    internal fun getOrDefault(rowType: KType): Decoder<*> {
        return decoderCache[rowType] ?: defaultDecoder
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrDefault(): Decoder<*> {
        return getOrDefault(typeOf<T>())
    }

    /**
     * Get a [Decoder] for the provided type [rowType]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    @PublishedApi
    internal fun getOrNull(rowType: KType): Decoder<*>? {
        return decoderCache[rowType]
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): Decoder<T>? {
        return getOrNull(typeOf<T>()) as Decoder<T>?
    }

    /** Add or replace an existing parser with a new [parser] for the [rowType] specified */
    @PublishedApi
    internal fun <T> insertOrReplace(rowType: KType, parser: Decoder<T>) {
        decoderCache[rowType.withNullability(false)] = parser
    }

    /** Add or replace an existing parser with a new [parser] for the [T] specified */
    inline fun <reified T> insertOrReplace(parser: Decoder<T>) {
        insertOrReplace(typeOf<T>(), parser)
    }

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    internal fun loadCache() {
        decoderCache
    }

    private val decoderInterfaceKClass = Decoder::class
    private val decoderInterfaceClass = decoderInterfaceKClass.java

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
            yield(parseDecoder(result, classInfo, kClass as KClass<Decoder<*>>))
        }
    }

    /**
     * Process and return a [Decoder] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     */
    private fun parseDecoder(
        scanResult: ScanResult,
        classInfo: ClassInfo,
        kClass: KClass<Decoder<*>>,
    ): Pair<KType, Decoder<*>> {
        val valueTypeName = classInfo.typeSignature
            .superinterfaceSignatures
            .first { it.fullyQualifiedClassName == decoderInterfaceClass.name }
            .typeArguments
            .first()
            .typeSignature
            .toString()
        val valueClassInfo = scanResult.getClassInfo(valueTypeName)
            ?: ClassGraph().enableAllInfo().acceptClasses(valueTypeName).scan().use {
                it.getClassInfo(valueTypeName)
            }
        val valueClass = if (valueClassInfo == null) {
            runCatching {
                DecoderCache::class.java.classLoader.loadClass(valueTypeName)
            }.getOrNull() ?: throw CannotFindDecodeValueType(valueTypeName)
        } else {
            valueClassInfo.loadClass()
        }.kotlin
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
        return valueClass.createType(nullable = false) to decoder
    }
}