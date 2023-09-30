package org.snappy.data

import org.snappy.ksp.RowParser

@RowParser
data class ResultPair(val first: Int, val second: String)
