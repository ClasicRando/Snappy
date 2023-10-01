@file:Suppress("UNCHECKED_CAST")

package org.snappy.cache

import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyConfig
import org.snappy.logging.logger
import org.snappy.rowparse.RowParser
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

/**
 * Cache of [RowParser] implementation for a desired row type. Backed by a readonly [HashMap] to
 * allow for multiple threads to access the cache at once.
 */
class RowParserCache internal constructor(
    config: SnappyConfig,
) : AbstractTypeCache<RowParser<*>>(config) {
    /** [KLogger][io.github.oshai.kotlinlogging.KLogger] for this cache instance */
    override val log by logger()

    /**
     * Get a cached [RowParser] for the provided type [T]. Checks the [cache] for an existing items
     * and returns immediately if it exists. Otherwise, it throws an [IllegalStateException]
     */
    inline fun <reified T : Any> getOrThrow(): RowParser<T> {
        return getOrThrow(typeOf<T>()) as RowParser<T>
    }

    /**
     * Get a cached [RowParser] for the provided type [T]. Checks the [cache] for an existing items
     * and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): RowParser<T>? {
        return getOrNull(typeOf<T>()) as RowParser<T>?
    }

    override val cacheType: KClass<RowParser<*>> = RowParser::class

    /**
     * Yield pairs of a [KType] and [RowParser] to initialize the cache with classes that extend
     * [RowParser]
     */
    override fun processAllAutoCacheClasses(result: ScanResult) = sequence {
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
            .first { it.jvmErasure.isSubclassOf(cacheType) }
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
