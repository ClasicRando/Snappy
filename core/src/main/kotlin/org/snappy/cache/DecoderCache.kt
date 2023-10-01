@file:Suppress("UNCHECKED_CAST")
package org.snappy.cache

import io.github.classgraph.ScanResult
import org.snappy.NoDefaultConstructor
import org.snappy.SnappyConfig
import org.snappy.TypeArgumentMismatch
import org.snappy.decode.Decoder
import org.snappy.logging.logger
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.createType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

class DecoderCache internal constructor(
    config: SnappyConfig,
): AbstractTypeCache<Decoder<*>>(config) {
    /** [KLogger][io.github.oshai.kotlinlogging.KLogger] for this cache instance */
    override val log by logger()

    /**
     * Get a [Decoder] for the provided type [T]. Checks the [cache] for an existing
     * parser and returns immediately if it exists. Otherwise, it returns null
     */
    inline fun <reified T : Any> getOrNull(): Decoder<T>? {
        return getOrNull(typeOf<T>()) as Decoder<T>?
    }

    override val cacheType = Decoder::class

    /**
     * Yield pairs of a [KType] and [Decoder] to initialize the cache with classes implementing
     * [Decoder]
     */
    override fun processAllAutoCacheClasses(result: ScanResult) = sequence {
        for (classInfo in result.getClassesImplementing(Decoder::class.java)) {
            if (
                classInfo.isAbstract ||
                (classInfo.name.contains("$") && !classInfo.name.contains("Companion"))
            ) {
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
            .first { it.jvmErasure.isSubclassOf(cacheType) }
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