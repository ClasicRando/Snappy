@file:Suppress("UNCHECKED_CAST")
package org.snappy.decode

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
import org.snappy.CannotFindDecodeValueType
import org.snappy.NoDefaultConstructor
import org.snappy.annotations.SnappyCacheRowParser
import org.snappy.SnappyConfig
import org.snappy.annotations.SnappyCacheDecoder
import org.snappy.decodeError
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
     * caches to be loaded immediately in a blocking but thread-safe manner. This reduces the first
     * load time of queries within the application.
     */
    internal fun loadCache() {
        decoderCache
    }

    private val decoderInterfaceKClass = Decoder::class
    private val decoderInterfaceClass = decoderInterfaceKClass.java

    /**
     * Yield pairs of a [KType] and [Decoder] to initialize the cache with classes marked as
     * [SnappyCacheDecoder]
     */
    private fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesWithAnnotation(SnappyCacheDecoder::class.java)) {
            yieldAll(processClassInfoForCache(result, classInfo))
        }
    }

    /**
     * Process a [classInfo] instance annotated with [SnappyCacheDecoder] to insert a [Decoder]
     *
     * @see SnappyCacheDecoder
     */
    private fun processClassInfoForCache(
        result: ScanResult,
        classInfo: ClassInfo,
    ) = sequence {
        val cls = classInfo.loadClass()
        val kClass = cls.kotlin
        if (decoderInterfaceClass.isAssignableFrom(cls)) {
            yieldAll(parseDecoder(result, classInfo, kClass as KClass<Decoder<*>>))
            return@sequence
        }
        kClass.companionObject?.let { companion ->
            if (companion.isSubclassOf(decoderInterfaceKClass)) {
                yieldAll(parseDecoder(
                    result,
                    result.getClassInfo(companion.java.name),
                    companion as KClass<Decoder<*>>
                ))
                return@sequence
            }
        }
        yield(kClass.createType(nullable = false) to generateDefaultDecoder(kClass))
        yield(kClass.createType(nullable = true) to generateDefaultDecoder(kClass))
    }

    /**
     * Process and return a [Decoder] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     *
     * @see SnappyCacheRowParser
     */
    private fun parseDecoder(
        scanResult: ScanResult,
        classInfo: ClassInfo,
        kClass: KClass<Decoder<*>>,
    ) = sequence {
        val valueTypeName = classInfo.typeSignature
            .superinterfaceSignatures
            .first { it.fullyQualifiedClassName == decoderInterfaceClass.name }
            .typeArguments
            .first()
            .typeSignature
            .toString()
        var valueCls = scanResult.getClassInfo(valueTypeName)
        if (valueCls == null) {
            valueCls = ClassGraph().enableAllInfo().acceptClasses(valueTypeName).scan().use {
                it.getClassInfo(valueTypeName)
            }
        }
        val valueClass = when {
            valueCls == null && valueTypeName.startsWith("java.") -> {
                DecoderCache::class.java.classLoader.loadClass(valueTypeName)
            }
            valueCls != null -> valueCls.loadClass()
            else -> throw CannotFindDecodeValueType(valueTypeName)
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
        yield(valueClass.createType() to decoder)
    }

    /** Create a new default decoder for the [valueClass] provided */
    private fun <T : Any> generateDefaultDecoder(valueType: KType): Decoder<T> {
        val valueClass = valueType.jvmErasure
        if (valueClass.isValue) {
            Decoder { value ->
                try {
                    valueClass.primaryConstructor!!.call(value)
                } catch (_: IllegalArgumentException) {
                    decodeError(valueClass, value)
                }
            }
        }
        return Decoder { value ->
            if (valueType.isMarkedNullable && value == null) {
                return@Decoder null
            }
            val valueCast = valueClass.safeCast(value) ?: decodeError(valueClass, value)
            valueCast as T?
        }
    }
}