package org.snappy

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.snappy.data.AnnotatedTestClass
import org.snappy.data.NonEmptyConstructorTestClass
import org.snappy.data.SimpleTestClass
import org.snappy.rowparse.DefaultRowParser
import org.snappy.rowparse.SnappyRow
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
        val row = mockk<SnappyRow>()
        every { row.containsKey(any()) } returns true
        every { row.getAnyNullable("stringField") } returns stringData
        every { row.getAnyNullable("booleanField") } returns booleanData
        every { row.getAnyNullable("shortField") } returns shortData
        every { row.getAnyNullable("intField") } returns intData
        every { row.getAnyNullable("longField") } returns longData
        every { row.getAnyNullable("doubleField") } returns doubleData
        every { row.getAnyNullable("floatField") } returns floatData
        every { row.getAnyNullable("nullField") } returns anyData

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
        val row = mockk<SnappyRow>()
        every { row.containsKey(any()) } returns false
        every { row.entries } returns sequenceOf()
        for ((key, value) in rowData) {
            every { row.containsKey(key) } returns true
            every { row.getAnyNullable(key) } returns value
        }

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
        val row = mockk<SnappyRow>()
        every { row.containsKey(any()) } returns true
        every { row.getAnyNullable("simple_name") } returns stringData
        every { row.getAnyNullable("otherFieldName") } returns longData

        val result = assertDoesNotThrow { annotatedParser.parseRow(row) }

        assertEquals(result.complexFieldName, stringData)
        assertEquals(result.otherFieldName, longData)
    }

    @Test
    fun `fromRow should fail when non empty constructor`() {
        val row = mockk<SnappyRow>()

        assertThrows<NoDefaultConstructor> { nonEmptyConstructorParser.parseRow(row) }
    }

    @Test
    fun `fromRow should fail when null set to non-null field`() {
        val row = mockk<SnappyRow>()
        every { row.containsKey(any()) } returns false
        every { row.entries } returns sequenceOf()
        every { row.containsKey("stringField") } returns true
        every { row.getAnyNullable("stringField") } returns null

        assertThrows<NullSet> { simpleParser.parseRow(row) }
    }

    @Test
    fun `fromRow should fail when wrong value type`() {
        val row = mockk<SnappyRow>()
        every { row.containsKey(any()) } returns false
        every { row.entries } returns sequenceOf()
        every { row.containsKey("stringField") } returns true
        every { row.getAnyNullable("stringField") } returns 1L

        assertThrows<MismatchSet> { simpleParser.parseRow(row) }
    }
}