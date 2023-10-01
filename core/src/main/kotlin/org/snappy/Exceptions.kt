package org.snappy

import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

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

class NullSet(name: String)
    : Throwable("Attempted to set value of a non-null property, '$name', with a null value")

class MismatchSet(prop: KProperty<*>, cls: KClass<*>) : Throwable(
    "Attempted to call a setter to a field, '${prop.name}', with the wrong value type. " +
    "Expected ${prop.returnType.jvmErasure.qualifiedName} found ${cls.qualifiedName}"
)

class NoDefaultConstructor(cls: KClass<*>)
    : Throwable("No default constructor available for the class '${cls.qualifiedName}'")

class BatchExecutionFailed(sql: String, batchNumber: UInt) : Throwable(
    """
        Batch SQL statement failed
        Batch Number: $batchNumber
        SQL: ${"\n" + sql.replaceIndent("        ")}
    """.trimIndent()
)

class DecodeError(decodeClassName: String?, value: Any?, valueType: String?) : Throwable(
    "Failed to decode value of type '$valueType' into '$decodeClassName', Value: $value"
)

inline fun <reified T> decodeError(value: Any?): Nothing {
    decodeError(T::class, value)
}

fun decodeError(decodeClass: KClass<*>, value: Any?): Nothing {
    throw DecodeError(decodeClass.qualifiedName, value, value?.let { it::class.qualifiedName })
}

class TypeArgumentMismatch(kClass: KClass<*>, kType: KType) : Throwable(
    "Decoder type , '$kType' has a different number of type arguments then the erased class $kClass"
)
