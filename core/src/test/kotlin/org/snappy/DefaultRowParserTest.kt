package org.snappy

import org.snappy.data.AnnotatedTestClass
import org.snappy.data.SimpleTestClass
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.snappy.data.NonEmptyConstructorTestClass
import kotlin.test.Test

import kotlin.test.assertEquals

class DefaultRowParserTest {
    private val simpleParser = DefaultRowParser(SimpleTestClass::class)
    private val annotatedParser = DefaultRowParser(AnnotatedTestClass::class)
    private val nonEmptyConstructorParser = DefaultRowParser(NonEmptyConstructorTestClass::class)

    @Suppress("RedundantNullableReturnType")
    @Test
    fun `fromRow should map row when simple class`() {
        val stringData = "String Data"
        val booleanData = false
        val shortData: Short = 1
        val intData = 23
        val longData = 5896L
        val doubleData = 52.63
        val floatData = 87.45F
        val anyData: Any? = "Any Data"
        val rowData = mapOf(
            "stringField" to stringData,
            "booleanField" to booleanData,
            "shortField" to shortData,
            "intField" to intData,
            "longField" to longData,
            "doubleField" to doubleData,
            "floatField" to floatData,
            "nullField" to anyData,
        )
        val row = SnappyRow(rowData)

        val result = assertDoesNotThrow { simpleParser.parseRow(row) }

        assertEquals(result.stringField, stringData)
        assertEquals(result.booleanField, booleanData)
        assertEquals(result.shortField, shortData)
        assertEquals(result.intField, intData)
        assertEquals(result.longField, longData)
        assertEquals(result.doubleField, doubleData)
        assertEquals(result.floatField, floatData)
        assertEquals(result.nullField, anyData)
    }

    @Test
    fun `fromRow should map row when missing value`() {
        val stringData = "String Data"
        val booleanData = false
        val intData = 23
        val longData = 5896L
        val doubleData = 52.63
        val floatData = 87.45F
        val rowData = mapOf(
            "stringField" to stringData,
            "booleanField" to booleanData,
            "intField" to intData,
            "longField" to longData,
            "doubleField" to doubleData,
            "floatField" to floatData,
        )
        val row = SnappyRow(rowData)

        val result = assertDoesNotThrow { simpleParser.parseRow(row) }

        assertEquals(result.stringField, stringData)
        assertEquals(result.booleanField, booleanData)
        assertEquals(result.intField, intData)
        assertEquals(result.longField, longData)
        assertEquals(result.doubleField, doubleData)
        assertEquals(result.floatField, floatData)
    }

    @Test
    fun `fromRow should map row when annotated type`() {
        val stringData = "String Data"
        val longData = 5896L
        val rowData = mapOf(
            "simple_name" to stringData,
            "otherFieldName" to longData,
        )
        val row = SnappyRow(rowData)

        val result = assertDoesNotThrow { annotatedParser.parseRow(row) }

        assertEquals(result.complexFieldName, stringData)
        assertEquals(result.otherFieldName, longData)
    }

    @Test
    fun `fromRow should fail when non empty constructor`() {
        val row = SnappyRow(mapOf())

        assertThrows<NoDefaultConstructor> { nonEmptyConstructorParser.parseRow(row) }
    }

    @Test
    fun `fromRow should fail when null set to non-null field`() {
        val row = SnappyRow(mapOf("stringField" to null))

        assertThrows<NullSet> { simpleParser.parseRow(row) }
    }

    @Test
    fun `fromRow should fail when wrong value type`() {
        val row = SnappyRow(mapOf("stringField" to 1L))

        assertThrows<MismatchSet> { simpleParser.parseRow(row) }
    }
}