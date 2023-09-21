package org.snappy

import org.snappy.rowparse.SnappyRow
import java.lang.Exception
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

/** Exception thrown when extracting a value from a [SnappyRow] but the type cast fails */
class WrongFieldType(name: String, typeName: String, value: String) : Throwable(
    "Failed to extract field '$name'$typeName. Found value $value"
)

fun wrongFieldType(name: String, typeName: String?, value: Any?): Nothing {
    throw WrongFieldType(
        name,
        if (typeName != null) " as $typeName" else "",
        "'$value' (${value?.let { it::class.qualifiedName } ?: ""})")
}

/** Exception thrown when parsing a [SnappyRow] and the key is not found */
class MissingField(name: String) : Throwable("Could not find field '$name' in row")

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

class InvalidDataClassConstructorCall(message: String) : Throwable(message)

internal fun invalidDataClassConstructorCall(
    parameterNames: List<String>,
    row: SnappyRow,
): Nothing {
    val displayRows = row.entries.joinToString("\n        ") { (key, value) ->
        "    $key: $value (${value?.let { it::class.qualifiedName}})"
    }
    val message = """
        Attempted to call a data class constructor with invalid parameter types.
        Parameter Names: ${parameterNames.joinToString()}
        Row:
        $displayRows
    """.trimIndent()
    throw InvalidDataClassConstructorCall(message)
}

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

class CannotFindDecodeValueType(typeName: String)
    : Exception("Cannot find decode value type '$typeName'")

class TypeArgumentMismatch(kClass: KClass<*>, kType: KType) : Throwable(
    "Decoder type , '$kType' has a different number of type arguments then the erased class $kClass"
)
