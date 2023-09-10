package org.snappy

import java.lang.IllegalArgumentException
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
            propertyAnnotations[parameter.name!!] ?: parameter.name!!
        }
        .toList()

    override fun parseRow(row: SnappyRow): T {
        return try {
            constructor.call(*parameterNames.map { row.get(it) }.toTypedArray())
        } catch (_: IllegalArgumentException) {
            throw invalidDataClassConstructorCall(parameterNames, row)
        }
    }
}
