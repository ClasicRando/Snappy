package org.snappy

import kotlin.reflect.KClass
import kotlin.reflect.KType

/**
 * Exception thrown when a [SqlParameter.Out][org.snappy.statement.SqlParameter.Out] is used outside
 * a procedure call
 */
class OutParameterOutsideProcedure
    : Throwable("Attempted to use an OUT parameter in a PreparedStatement")

/** Exception thrown when a column name in a [java.sql.ResultSet] is null */
class NullFieldName : Throwable("Found a null column name in a ResultSet")

/** Exception thrown when a query expects to return a single row but multiple rows are found */
class TooManyRows : Throwable("Result set contained too many rows")

/** Exception thrown when a query expects a non-empty result but no rows are returned */
class EmptyResult : Throwable("Expected result to contain rows but none were found")

/**
 * Exception thrown when a [java.sql.Statement]'s results have been exhausted but another call to
 * fetch more results is made.
 */
class NoMoreResults
    : Throwable("Attempted to access more results from a statement that has already been exhausted")

/** Exception thrown when a decoder returns a null value but a non-null value was expected */
class NullSet(name: String)
    : Throwable("Attempted to set value of a non-null property, '$name', with a null value")

/**
 * Exception thrown when a cache class attempts to create a new cache item entry but there is no
 * default constructor
 */
class NoDefaultConstructor(cls: KClass<*>)
    : Throwable("No default constructor available for the class '${cls.qualifiedName}'")

/**
 * Exception thrown when a batch sql statement fails. The batch number of sql statement are provided
 * in the log. NOTE: This will only occur when the caller to a batch sql statement explicitly turns
 * on checking the batch execution result for failed result flags.
 */
class BatchExecutionFailed(sql: String, batchNumber: UInt) : Throwable(
    """
        Batch SQL statement failed
        Batch Number: $batchNumber
        SQL: ${"\n" + sql.replaceIndent("        ")}
    """.trimIndent()
)

/**
 * Exception thrown when attempting to decode a value but the underlining value is not of the
 * required type
 */
class DecodeError(decodeClassName: String?, value: Any?, valueType: String?) : Throwable(
    "Failed to decode value of type '$valueType' into '$decodeClassName', Value: $value"
)

/** Throw a [DecodeError] with the provided [value] and expected type [T] */
inline fun <reified T> decodeError(value: Any?): Nothing {
    decodeError(T::class, value)
}

/** Throw a [DecodeError] with the provided [value] and expected type class */
fun decodeError(decodeClass: KClass<*>, value: Any?): Nothing {
    throw DecodeError(decodeClass.qualifiedName, value, value?.let { it::class.qualifiedName })
}

/**
 * Exception thrown when checking a [KType] against a jvm erased class to ensure they have the same
 * number of generic type arguments. This in theory should never be thrown
 */
class TypeArgumentMismatch(kClass: KClass<*>, kType: KType) : Throwable(
    "Decoder type , '$kType' has a different number of type arguments then the erased class $kClass"
)
