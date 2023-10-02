package org.snappy

import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.impl.annotations.MockK
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.snappy.extensions.columnNames
import java.sql.ResultSet
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ResultSetExtensionsTest {
    @MockK
    lateinit var resultSet: ResultSet

    @BeforeTest
    fun setUp() = MockKAnnotations.init(this)

    @Test
    fun `columnNames should fail when result set contains column with null name`() {
        every { resultSet.metaData.columnCount } returns 1
        every { resultSet.metaData.getColumnName(1) } returns null

        assertThrows<NullFieldName> { resultSet.columnNames }
    }

    @Test
    fun `columnNames should return list of names when valid result set`() {
        val columnSize = 4
        val fieldName = "Test Field"
        every { resultSet.metaData.columnCount } returns columnSize
        every { resultSet.metaData.getColumnName(any()) } returns fieldName

        val columnNames = assertDoesNotThrow { resultSet.columnNames }

        assertTrue(columnNames.isNotEmpty())
        assertEquals(columnSize, columnNames.size)
        assertTrue(columnNames.all { it == fieldName })
    }
}