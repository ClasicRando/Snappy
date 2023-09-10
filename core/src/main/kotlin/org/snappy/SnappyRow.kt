package org.snappy

import kotlin.reflect.KClass
import kotlin.reflect.safeCast

/**
 * Internal representation of a row from a [java.sql.ResultSet] to not expose the result to a user.
 * Access of data is through the field name.
 */
class SnappyRow(private val data: Map<String, Any?>) {

    /** Number of columns in the row */
    val size: Int = data.size
    /** Read-only view of the row entries */
    val entries: Sequence<Map.Entry<String, Any?>> = data.asSequence()

    /** Check if a row contains the specified [key] */
    fun containsKey(key: String) = data.containsKey(key)

    /**
     * Get the value associated with the [key] as [T] or null if the underlining value is null
     *
     * @exception WrongFieldType when the value is not of type [T]
     */
    inline fun <reified T : Any> getAs(key: String): T? {
        return getAs(key, T::class)
    }

    /**
     * Get the value associated with the [key] as [T] or null if the underlining value is null
     *
     * @exception WrongFieldType when the value is not of type [T]
     */
    @PublishedApi
    internal fun <T : Any> getAs(key: String, returnType: KClass<T>): T? {
        val value = data[key] ?: return null
        return returnType.safeCast(value) ?: throw WrongFieldType(key, returnType.simpleName)
    }

    /** Get the value associated with the [key] */
    fun get(key: String): Any? = data[key]
}
