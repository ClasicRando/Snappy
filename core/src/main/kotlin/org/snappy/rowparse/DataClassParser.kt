package org.snappy.rowparse

import org.snappy.SnappyMapper
import org.snappy.annotations.SnappyColumn
import org.snappy.decode.decodeWithType
import org.snappy.invalidDataClassConstructorCall
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor

/**
 * Default [RowParser] for data class objects.
 *
 * Collects metadata about the class specified to create a [RowParser] implementation for the
 * data class. Utilizes the primary constructor of the data class, passing all constructor
 * parameters as values found in the [SnappyRow] provided. For ease of mapping query fields to
 * properties, the [SnappyColumn] annotation is checked for parameters to provide custom mapping.
 *
 * A call to [parseRow] can fail if the row provided does not contain the required parameter or the
 * call to the constructor function does not succeed.
 */
class DataClassParser<T : Any>(rowClass: KClass<T>) : RowParser<T> {
    /** Constructor function for the data class */
    private val constructor = rowClass.primaryConstructor!!
    /** Map of property name to alias specified by a [SnappyColumn] annotation */
    private val propertyAnnotations = rowClass.memberProperties
        .asSequence()
        .mapNotNull { prop ->
            val annotation = prop.annotations
                .firstNotNullOfOrNull { it as? SnappyColumn }
                ?: return@mapNotNull null
            prop.name to annotation.name
        }
        .toMap()
    /**
     * Names of the parameters (or alias if [SnappyColumn] annotation found) to fetch from a
     * [SnappyRow] when calling [parseRow].
     */
    private val parameterNames = constructor.parameters
        .asSequence()
        .map { parameter ->
            val name = propertyAnnotations[parameter.name!!] ?: parameter.name!!
            val decoder = SnappyMapper.decoderCache.getOrDefault(parameter.type)
            Triple(name, parameter, decoder)
        }
        .toList()

    override fun parseRow(row: SnappyRow): T {
        val parameters = parameterNames.map { (name, parameter, decoder) ->
            if (!row.containsKey(name)) {
                invalidDataClassConstructorCall(parameterNames.map { it.first }, row)
            }
            decoder.decodeWithType(parameter.type, parameter.name!!, row.get(name))
        }.toTypedArray()
        return try {
            constructor.call(*parameters)
        } catch (_: IllegalArgumentException) {
            invalidDataClassConstructorCall(parameterNames.map { it.first }, row)
        }
    }
}
