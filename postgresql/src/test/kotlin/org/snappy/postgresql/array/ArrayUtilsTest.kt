package org.snappy.postgresql.array

import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.postgresql.PGConnection
import org.postgresql.jdbc.PgConnection
import org.snappy.DecodeError
import org.snappy.decode.Decoder
import org.snappy.execute.execute
import org.snappy.postgresql.data.SimpleCompositeTest
import java.lang.IllegalStateException
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "SNAPPY_PG_TEST", matches = "true")
class ArrayUtilsTest {
    private val missingEnvironmentVariableMessage = "To run Postgres tests the environment " +
            "variable SNAPPY_PG_CONNECTION_STRING must be available"

    private inline fun useConnection(action: (PgConnection) -> Unit) {
        val connectionString = System.getenv("SNAPPY_PG_CONNECTION_STRING")
            ?: throw IllegalStateException(missingEnvironmentVariableMessage)
        DriverManager.getConnection(connectionString).unwrap(PgConnection::class.java).use(action)
    }

    @Test
    fun `SQL Array toListWithNulls should fail when array property is not Array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns ""

        assertThrows<ClassCastException> { array.toListWithNulls<String>() }
    }

    @Test
    fun `SQL Array toListWithNulls should fail when decoded item cannot be cast`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        assertThrows<DecodeError> { array.toListWithNulls<Int>() }
    }

    @Test
    fun `SQL Array toListWithNulls should return string list when array backed by string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        val list = assertDoesNotThrow { array.toListWithNulls<String>() }
        assertTrue(list.isNotEmpty())
        assertNotNull(list.first())
        assertEquals("Test", list[0])
    }

    @Test
    fun `SQL Array toListWithNulls should return string list when array backed by nullable string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf<String?>(null)

        val list = assertDoesNotThrow { array.toListWithNulls<String>() }
        assertTrue(list.isNotEmpty())
        assertNull(list.first())
    }

    @Test
    fun `SQL Array toList should fail when item is null`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf<String?>(null)

        assertThrows<IllegalStateException> { array.toList<String>() }
    }

    @Test
    fun `SQL Array toList should return string list when array backed by string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        val list = assertDoesNotThrow { array.toList<String>() }
        assertTrue(list.isNotEmpty())
        assertEquals("Test", list[0])
    }




    @Test
    fun `SQL Array toArrayWithNulls should fail when array property is not Array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns ""

        assertThrows<ClassCastException> { array.toArrayWithNulls<String>() }
    }

    @Test
    fun `SQL Array toArrayWithNulls should fail when decoded item cannot be cast`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        assertThrows<DecodeError> { array.toArrayWithNulls<Int>() }
    }

    @Test
    fun `SQL Array toArrayWithNulls should return string list when array backed by string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        val list = assertDoesNotThrow { array.toArrayWithNulls<String>() }
        assertTrue(list.isNotEmpty())
        assertNotNull(list.first())
        assertEquals("Test", list[0])
    }

    @Test
    fun `SQL Array toArrayWithNulls should return string list when array backed by nullable string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf<String?>(null)

        val list = assertDoesNotThrow { array.toArrayWithNulls<String>() }
        assertTrue(list.isNotEmpty())
        assertNull(list.first())
    }

    @Test
    fun `SQL Array toArray should fail when item is null`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf<String?>(null)

        assertThrows<IllegalStateException> { array.toArray<String>() }
    }

    @Test
    fun `SQL Array toArray should return string list when array backed by string array`() {
        val array = mockk<java.sql.Array>()
        every { array.array } returns arrayOf("Test")

        val list = assertDoesNotThrow { array.toArray<String>() }
        assertTrue(list.isNotEmpty())
        assertEquals("Test", list[0])
    }

    @Test
    fun `Array toPgArray should fail when unknown type`() {

        assertThrows<CannotEncodeArray> {
            useConnection {
                val array = arrayOf<Decoder<String>>().toPgArray(it)
                it.execute("select ?::int[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when boolean`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(true, false).toPgArray(it)
                it.execute("select ?::bool[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when short`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf<Short>(1, 6).toPgArray(it)
                it.execute("select ?::smallint[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when int`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(1, 5).toPgArray(it)
                it.execute("select ?::int[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when long`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(55L, 86L).toPgArray(it)
                it.execute("select ?::bigint[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when float`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(4F, 89F).toPgArray(it)
                it.execute("select ?::real[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when double`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(4.256, 8.653).toPgArray(it)
                it.execute("select ?::double precision[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when byte`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf<Byte>(0x56, 0x78).toPgArray(it)
                it.execute("select ?::bytea[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when string`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf("Test", "Test").toPgArray(it)
                it.execute("select ?::text[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when big decimal`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(BigDecimal("58.42"), BigDecimal("549")).toPgArray(it)
                it.execute("select ?::numeric[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when sql date`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(
                    Date.valueOf(LocalDate.of(2023, 1, 1)),
                    Date.valueOf(LocalDate.of(2023, 5, 1)),
                ).toPgArray(it)
                it.execute("select ?::date[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when sql timestamp`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(Timestamp.from(Instant.now())).toPgArray(it)
                it.execute("select ?::timestamp[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when local time`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(LocalTime.now()).toPgArray(it)
                it.execute("select ?::time[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when offset time`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(OffsetTime.now()).toPgArray(it)
                it.execute("select ?::timetz[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when local date`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(LocalDate.now()).toPgArray(it)
                it.execute("select ?::date[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when local date time`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(LocalDateTime.now()).toPgArray(it)
                it.execute("select ?::timestamp[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when offset date time`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(OffsetDateTime.now()).toPgArray(it)
                it.execute("select ?::timestamptz[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when instant`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(Instant.now()).toPgArray(it)
                it.execute("select ?::timestamptz[]", listOf(array))
            }
        }
    }

    @Test
    fun `Array toPgArray should succeed when class convertible to PGobject`() {
        assertDoesNotThrow {
            useConnection {
                val array = arrayOf(SimpleCompositeTest.default).toPgArray(it)
                it.execute("select ?::simple_composite_test[]", listOf(array))
            }
        }
    }
}
