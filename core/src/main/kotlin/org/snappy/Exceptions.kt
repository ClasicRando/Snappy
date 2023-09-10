package org.snappy

import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.jvm.jvmErasure

/** Exception thrown when a [org.snappy.SqlParameter.Out] is used outside a procedure call */
class OutParameterOutsideProcedure
    : Throwable("Attempted to use an OUT parameter in a PreparedStatement")

/** Exception thrown when extracting a value from a [SnappyRow] but the type cast fails */
class WrongFieldType(name: String, typeName: String?)
    : Throwable("Failed to extract field '$name'${ if (typeName != null) " as $typeName" else "" }")

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

/** Exception thrown when parsing a [SnappyRow] using [SnappyRow.getAs] but the value was null */
class NullRowValue(name: String)
    : Throwable("Assertion of a non-null value when parsing a row for key '$name' failed")

class NullSet(name: String)
    : Throwable("Attempted to call a setter of a non-null field, '$name', with a null value")

class MismatchSet(prop: KProperty<*>, cls: KClass<*>) : Throwable(
    "Attempted to call a setter to a field, '${prop.name}', with the wrong value type. " +
    "Expected ${prop.returnType.jvmErasure.qualifiedName} found ${cls.qualifiedName}"
)

class NoDefaultConstructor(cls: KClass<*>)
    : Throwable("No default constructor available for the class '${cls.qualifiedName}'")

class InvalidDataClassConstructorCall(message: String) : Throwable(message)

internal fun invalidDataClassConstructorCall(
    parameterNames: List<String>,
    row: SnappyRow,
): InvalidDataClassConstructorCall {
    val displayRows = row.entries.joinToString("\n        ") { (key, value) ->
        "    $key: $value (${value?.let { it::class.qualifiedName}})"
    }
    val message = """
        Attempted to call a data class constructor with invalid parameter types.
        Parameter Names: ${parameterNames.joinToString()}
        Row:
        $displayRows
    """.trimIndent()
    return InvalidDataClassConstructorCall(message)
}
