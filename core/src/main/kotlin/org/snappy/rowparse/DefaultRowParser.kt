package org.snappy.rowparse

import org.snappy.MismatchSet
import org.snappy.NoDefaultConstructor
import org.snappy.NullSet
import org.snappy.SnappyColumn
import org.snappy.SnappyRow
import kotlin.reflect.KClass
import kotlin.reflect.KMutableProperty
import kotlin.reflect.KVisibility
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.memberProperties

/**
 * Default [RowParser] for POJO class objects.
 *
 * Collects metadata about the class specified to create a [RowParser] implementation for the
 * class. Assumes that the class contains a zero parameter constructor and publicly settable fields.
 * For ease of mapping query fields to properties, the [SnappyColumn] annotation is checked for
 * mutable properties to provide custom mapping.
 *
 * A call to [parseRow] can fail if:
 * - the class does not have a default constructor (with zero parameters)
 * - a property cannot be set using the mutable property's setter
 * - the value extracted from the row does not match the properties type
 */
class DefaultRowParser<T : Any>(private val rowClass: KClass<T>) : RowParser<T> {
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
     * Publicly mutable properties that will be updated with row contents (if the property name or
     * alias is found within the row)
     */
    private val properties = rowClass.memberProperties
        .asSequence()
        .filter { prop ->
            prop.visibility?.let { it == KVisibility.PUBLIC } ?: false
        }
        .mapNotNull { prop -> prop as? KMutableProperty<*> }
        .map { prop ->
            (propertyAnnotations[prop.name] ?: prop.name) to prop
        }
        .toList()

    override fun parseRow(row: SnappyRow): T {
        val newInstance = try {
            rowClass.createInstance()
        } catch (_: IllegalArgumentException) {
            throw NoDefaultConstructor(rowClass)
        }
        properties.asSequence()
            .filter { (name, _) -> row.containsKey(name) }
            .forEach { (name, prop) ->
                val value = row.get(name)
                if (value == null && !prop.returnType.isMarkedNullable) {
                    throw NullSet(prop.name)
                }
                try {
                    prop.setter.call(newInstance, value)
                } catch (_: IllegalArgumentException) {
                    throw MismatchSet(prop, value!!::class)
                }
            }
        return newInstance
    }
}
