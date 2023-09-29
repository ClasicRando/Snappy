package org.snappy.postgresql.data

import org.postgresql.util.PGobject
import org.snappy.postgresql.type.PgObjectDecoder
import org.snappy.postgresql.type.PgType
import org.snappy.postgresql.type.ToPgObject
import org.snappy.postgresql.type.parseComposite
import kotlin.reflect.KClass

@PgType("complex_composite_test")
data class ComplexCompositeTest(
    val intField: Int,
    val textField: String,
    val compositeField: SimpleCompositeTest,
    val intArrayField: List<Int?>,
    val compositeArrayField: List<SimpleCompositeTest?>,
) : ToPgObject {
    override fun toPgObject(): PGobject {
        TODO()
    }

    companion object : PgObjectDecoder<ComplexCompositeTest> {
        override fun decodePgObject(pgObject: PGobject): ComplexCompositeTest? {
            return parseComposite(pgObject) {
                val textField = readString() ?: error("string field cannot be null")
                val intField = readInt() ?: error("int field cannot be null")
                val compositeField = readComposite<SimpleCompositeTest>()
                    ?: error("composite field cannot be null")
                val intArrayField = readList<Int>() ?: error("int array cannot be null")
                val compositeArrayField = readList<SimpleCompositeTest>()
                    ?: error("composite array field cannot be null")
                ComplexCompositeTest(
                    intField,
                    textField,
                    compositeField,
                    intArrayField,
                    compositeArrayField,
                )
            }
        }
    }
}
