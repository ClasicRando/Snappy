package org.snappy.data

import org.snappy.ksp.RowParser

@RowParser
class SimpleTestClass {
    var stringField: String = ""
    var booleanField: Boolean = false
    var shortField: Short = 0
    var intField: Int = 0
    var longField: Long = 0
    var doubleField: Double = 0.0
    var floatField: Float = 0.0F
    var nullField: Any? = null
}