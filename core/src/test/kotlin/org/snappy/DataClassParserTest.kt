package org.snappy

import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.snappy.data.AnnotatedTestDataClass
import org.snappy.data.SimpleTestDataClass
import kotlin.test.Test
import kotlin.test.assertEquals

class DataClassParserTest {

    private val simpleDataClassParser = DataClassParser(SimpleTestDataClass::class)
    private val annotatedDataClassParser = DataClassParser(AnnotatedTestDataClass::class)

    @Test
    fun `fromRow should map row when simple data class`() {
        val stringData = "String Data"
        val booleanData = false
        val shortData: Short = 1
        val intData = 23
        val longData = 5896L
        val doubleData = 52.63
        val floatData = 87.45F
        val nullData: Any? = null
        val rowData = mapOf(
            "stringField" to stringData,
            "booleanField" to booleanData,
            "shortField" to shortData,
            "intField" to intData,
            "longField" to longData,
            "doubleField" to doubleData,
            "floatField" to floatData,
            "nullField" to nullData,
        )
        val row = SnappyRow(rowData)
        val result = simpleDataClassParser.parseRow(row)

        assertEquals(result.stringField, stringData)
        assertEquals(result.booleanField, booleanData)
        assertEquals(result.shortField, shortData)
        assertEquals(result.intField, intData)
        assertEquals(result.longField, longData)
        assertEquals(result.doubleField, doubleData)
        assertEquals(result.floatField, floatData)
        assertEquals(result.nullField, nullData)
    }

    @Test
    fun `fromRow should fail when missing value`() {
        val stringData = "String Data"
        val booleanData = false
        val shortData: Short = 1
        val intData = 23
        val longData = 5896L
        val doubleData = 52.63
        val rowData = mapOf(
            "stringField" to stringData,
            "booleanField" to booleanData,
            "shortField" to shortData,
            "intField" to intData,
            "longField" to longData,
            "doubleField" to doubleData,
        )
        val row = SnappyRow(rowData)

        assertThrows<InvalidDataClassConstructorCall> { simpleDataClassParser.parseRow(row) }
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

        val result = assertDoesNotThrow { annotatedDataClassParser.parseRow(row) }

        assertEquals(result.complexFieldName, stringData)
        assertEquals(result.otherFieldName, longData)
    }
}