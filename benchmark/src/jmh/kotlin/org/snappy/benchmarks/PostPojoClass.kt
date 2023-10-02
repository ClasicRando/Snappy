package org.snappy.benchmarks

import org.snappy.ksp.symbols.RowParser
import java.time.LocalDateTime

@RowParser
class PostPojoClass {
    var id: Int = 0
    var text: String = ""
    var creationDate: LocalDateTime = LocalDateTime.MIN
    var lastChangeDate: LocalDateTime = LocalDateTime.MIN
    var counter1: Int? = null
    var counter2: Int? = null
    var counter3: Int? = null
    var counter4: Int? = null
    var counter5: Int? = null
    var counter6: Int? = null
    var counter7: Int? = null
    var counter8: Int? = null
    var counter9: Int? = null
}