@file:Suppress("UNCHECKED_CAST")

package org.snappy.rowparse

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.annotations.SnappyCacheRowParser
import org.snappy.SnappyConfig
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.companionObject
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
    /** Internally used flag to indicate if the cache has been loaded */
    internal var cacheLoaded: Boolean = false
        private set
    /** Map of an output type linked to a [RowParser] */
    private val rowParserCache: ConcurrentHashMap<KType, RowParser<*>> by lazy {
        val cache = ConcurrentHashMap<KType, RowParser<*>>()
        val packages = config.basePackages.toTypedArray()
        ClassGraph().enableAllInfo().acceptPackages(*packages).scan().use { result ->
            processAllAutoCacheClasses(result).toMap(cache)
        }
        cacheLoaded = true
        cache
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [rowType] and returns that new parser.
     */
    @PublishedApi
    internal fun <T : Any> getOrDefault(rowType: KType): RowParser<T> {
        val cachedResult = rowParserCache.getOrPut(rowType) {
            generateDefaultParser(rowType.jvmErasure)
        }
        return cachedResult as RowParser<T>
    }

    /**
     * Get a [RowParser] for the provided type [T]. Checks the [rowParserCache] for an existing
     * parser and returns immediately if it exists. Otherwise, adds a new default parser for the
     * required [T] and returns that new parser.
     */
    inline fun <reified T : Any> getOrDefault(): RowParser<T> {
        return getOrDefault(typeOf<T>())
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

    /** Add or replace an existing parser with a new [parser] for the [rowType] specified */
    @PublishedApi
    internal fun <T> insertOrReplace(rowType: KType, parser: RowParser<T>) {
        rowParserCache[rowType] = parser
    }

    /** Add or replace an existing parser with a new [parser] for the [T] specified */
    inline fun <reified T> insertOrReplace(parser: RowParser<T>) {
        insertOrReplace(typeOf<T>(), parser)
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
    private val rowParserInterfaceClass = rowParserInterfaceKClass.java

    /**
     * Yield pairs of a [KType] and [RowParser] to initialize the cache with classes marked as
     * [SnappyCacheRowParser]
     */
    private fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesWithAnnotation(SnappyCacheRowParser::class.java)) {
            if (classInfo.isAbstract) {
                continue
            }
            processClassInfoForCache(classInfo)?.let { yield(it) }
        }
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
            yield(createRowParserClass(result, classInfo, kClass as KClass<RowParser<*>>))
        }
    }

    /**
     * Process a [classInfo] instance annotated with [SnappyCacheRowParser] to insert a [RowParser].
     * If the class itself is already a [RowParser] or the companion object is a [RowParser] then
     * null is returned since the second pass of the results will cover those classes.
     *
     * @see SnappyCacheRowParser
     */
    private fun processClassInfoForCache(classInfo: ClassInfo): Pair<KType, RowParser<*>>? {
        val cls = classInfo.loadClass()
        val kClass = cls.kotlin
        if (rowParserInterfaceClass.isAssignableFrom(cls)) {
            return null
        }
        kClass.companionObject?.let { companion ->
            if (companion.isSubclassOf(rowParserInterfaceKClass)) {
                return null
            }
        }
        return kClass.createType() to generateDefaultParser(kClass)
    }

    /**
     * Process and return a [RowParser] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     *
     * @see SnappyCacheRowParser
     */
    private fun createRowParserClass(
        scanResult: ScanResult,
        classInfo: ClassInfo,
        kClass: KClass<RowParser<*>>,
    ): Pair<KType, RowParser<*>> {
        val rowType = classInfo.typeSignature
            .superinterfaceSignatures
            .first { it.fullyQualifiedClassName == rowParserInterfaceClass.name }
            .typeArguments
            .first()
            .typeSignature
        val rowClass = try {
            scanResult.getClassInfo(rowType.toString())
                .loadClass()
                .kotlin
        } catch (ex: Exception) {
            throw ex
        }
        val rowParser = try {
            if (kClass.isCompanion) {
                kClass.objectInstance
                    ?: throw IllegalStateException("Companion object must have object instance")
            } else {
                kClass.createInstance()
            }
        } catch (_: IllegalArgumentException) {
            throw NoDefaultConstructor(rowClass)
        }
        return rowClass.createType() to rowParser
    }

    /** Create a new default parser for the [rowClass] provided */
    private fun <T : Any> generateDefaultParser(rowClass: KClass<T>): RowParser<T> {
        if (rowClass.isData) {
            return DataClassParser(rowClass)
        }
        return DefaultRowParser(rowClass)
    }
}
