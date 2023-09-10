@file:Suppress("UNCHECKED_CAST")

package org.snappy

import io.github.classgraph.ClassGraph
import io.github.classgraph.ClassInfo
import io.github.classgraph.ScanResult
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
 * Global cache of [RowParser] implementation for a desired row type. Backed by a
 * [ConcurrentHashMap] to allow for multiple threads to access the cache at once.
 */
object RowParserCache {
    /** Map of an output type linked to a [RowParser] */
    private val rowParserCache = ConcurrentHashMap<KType, RowParser<*>>()

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

    private val rowParserInterfaceKClass = RowParser::class
    private val rowParserInterfaceClass = rowParserInterfaceKClass.java

    /**
     * Load all [RowParser] implementations marked as [SnappyAutoCache] under the [basePackage]
     * specified.
     *
     * This function can fail if the class marked as [SnappyAutoCache] can not be extracted into a
     * [RowParser] class.
     *
     * @see SnappyAutoCache
     */
    fun loadAutoCacheClasses(basePackage: String) {
        ClassGraph().enableAllInfo().acceptPackages(basePackage).scan().use { result ->
            result.getClassesWithAnnotation(SnappyAutoCache::class.java)
                .forEach { processClassInfoForCache(result, it) }
        }
    }

    /**
     * Process a [classInfo] instance annotated with [SnappyAutoCache] to insert a [RowParser]
     *
     * @see SnappyAutoCache
     */
    private fun processClassInfoForCache(result: ScanResult, classInfo: ClassInfo) {
        val cls = classInfo.loadClass()
        val kClass = cls.kotlin
        if (rowParserInterfaceClass.isAssignableFrom(cls)) {
            insertRowParserClass(result, classInfo, kClass as KClass<RowParser<*>>)
            return
        }
        kClass.companionObject?.let { companion ->
            if (companion.isSubclassOf(rowParserInterfaceKClass)) {
                insertRowParserClass(
                    result,
                    result.getClassInfo(companion.java.name),
                    companion as KClass<RowParser<*>>)
                return
            }
        }
        insertDefaultParser(kClass)
    }

    /**
     * Process and insert a [RowParser] for the specified [kClass], using reflection to get the row
     * type for cache insertion.
     *
     * @see SnappyAutoCache
     */
    private fun insertRowParserClass(
        scanResult: ScanResult,
        classInfo: ClassInfo,
        kClass: KClass<RowParser<*>>,
    ) {
        val rowType = classInfo.typeSignature
            .superinterfaceSignatures
            .first { it.fullyQualifiedClassName == rowParserInterfaceClass.name }
            .typeArguments
            .first()
            .typeSignature
        val rowClass = scanResult.getClassInfo(rowType.toString())
            .loadClass()
            .kotlin
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
        insertOrReplace(rowClass.createType(), rowParser)
    }

    /** Insert a default parser for the row type [kClass] provided */
    private fun insertDefaultParser(kClass: KClass<*>) {
        val rowParser = generateDefaultParser(kClass)
        val rowType = kClass.createType()
        rowParserCache[rowType] = rowParser
    }

    /** Create a new default parser for the [rowClass] provided */
    private fun <T : Any> generateDefaultParser(rowClass: KClass<T>): RowParser<T> {
        if (rowClass.isData) {
            return DataClassParser(rowClass)
        }
        return DefaultRowParser(rowClass)
    }
}
