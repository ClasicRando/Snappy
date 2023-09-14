@file:Suppress("UNCHECKED_CAST")
package org.snappy.decode

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyAutoCache
import org.snappy.SnappyConfig
import org.snappy.DecodeError
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.safeCast
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

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [rowType] and returns that new parser.
     */
    @PublishedApi
    internal fun <T : Any> getOrDefault(rowType: KType): Decoder<T> {
        val cachedResult = decoderCache.getOrPut(rowType) {
            generateDefaultDecoder(rowType.jvmErasure)
        }
        return cachedResult as Decoder<T>
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [T] and returns that new parser.
     */
    inline fun <reified T : Any> getOrDefault(): Decoder<T> {
        return getOrDefault(typeOf<T>())
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    @PublishedApi
    internal fun <T : Any> getOrNull(rowType: KType): Decoder<T>? {
        return decoderCache[rowType] as? Decoder<T>
    }

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [decoderCache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): Decoder<T>? {
        return getOrNull(typeOf<T>())
    }

    /** Add or replace an existing parser with a new [parser] for the [rowType] specified */
    @PublishedApi
    internal fun <T> insertOrReplace(rowType: KType, parser: Decoder<T>) {
        decoderCache[rowType] = parser
    }

    /** Add or replace an existing parser with a new [parser] for the [T] specified */
    inline fun <reified T> insertOrReplace(parser: Decoder<T>) {
        insertOrReplace(typeOf<T>(), parser)
    }

    /**
     * Method to ensure the cache is loaded before continuing. This will force the lazy initialized
     * to be loaded immateriality in a blocking but thread-safe manner. This reduces the first load
     * time of query within the application.
     */
    internal fun loadCache() {
        decoderCache
    }

    private val decoderInterfaceKClass = Decoder::class
    private val decoderInterfaceClass = decoderInterfaceKClass.java

    /**
     * Yield pairs of a [KType] and [Decoder] to initialize the cache with classes marked as
     * [SnappyAutoCache]
     */
    private fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesWithAnnotation(SnappyAutoCache::class.java)) {
            yield(processClassInfoForCache(result, classInfo))
        }
    }

    /**
     * Process a [classInfo] instance annotated with [SnappyAutoCache] to insert a [Decoder]
     *
     * @see SnappyAutoCache
     */
    private fun processClassInfoForCache(
        result: ScanResult,
        classInfo: ClassInfo,
    ): Pair<KType, Decoder<*>> {
        val cls = classInfo.loadClass()
        val kClass = cls.kotlin
        if (decoderInterfaceClass.isAssignableFrom(cls)) {
            return insertDecoderClass(result, classInfo, kClass as KClass<Decoder<*>>)
        }
        kClass.companionObject?.let { companion ->
            if (companion.isSubclassOf(decoderInterfaceKClass)) {
                return insertDecoderClass(
                    result,
                    result.getClassInfo(companion.java.name),
                    companion as KClass<Decoder<*>>
                )
            }
        }
        return kClass.createType() to generateDefaultDecoder(kClass)
    }

    /**
     * Process and return a [Decoder] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     *
     * @see SnappyAutoCache
     */
    private fun insertDecoderClass(
        scanResult: ScanResult,
        classInfo: ClassInfo,
        kClass: KClass<Decoder<*>>,
    ): Pair<KType, Decoder<*>> {
        val rowType = classInfo.typeSignature
            .superinterfaceSignatures
            .first { it.fullyQualifiedClassName == decoderInterfaceClass.name }
            .typeArguments
            .first()
            .typeSignature
        val rowClass = scanResult.getClassInfo(rowType.toString())
            .loadClass()
            .kotlin
        val decoder = try {
            if (kClass.isCompanion) {
                kClass.objectInstance
                    ?: throw IllegalStateException("Companion object must have object instance")
            } else {
                kClass.createInstance()
            }
        } catch (_: IllegalArgumentException) {
            throw NoDefaultConstructor(rowClass)
        }
        return rowClass.createType() to decoder
    }

    /** Create a new default decoder for the [valueClass] provided */
    private fun <T : Any> generateDefaultDecoder(valueClass: KClass<T>): Decoder<T> {
        if (valueClass.isValue) {
            Decoder { value ->
                try {
                    valueClass.primaryConstructor!!.call(value)
                } catch (_: IllegalArgumentException) {
                    throw DecodeError(
                        valueClass.qualifiedName,
                        value?.let { it::class.qualifiedName },
                    )
                }
            }
        }
        return Decoder { value ->
            valueClass.safeCast(value) ?: throw DecodeError(
                valueClass.qualifiedName,
                value?.let { it::class.qualifiedName }
            )
        }
    }
}