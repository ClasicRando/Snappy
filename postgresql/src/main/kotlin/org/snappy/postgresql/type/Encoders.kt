package org.snappy.postgresql.type

import org.postgresql.geometric.PGbox
import org.postgresql.geometric.PGcircle
import org.postgresql.geometric.PGline
import org.postgresql.geometric.PGlseg
import org.postgresql.geometric.PGpath
import org.postgresql.geometric.PGpoint
import org.postgresql.geometric.PGpolygon
import org.postgresql.util.PGInterval
import org.postgresql.util.PGmoney
import org.snappy.encode.Encode

fun PGInterval.encode(): Encode = ToPgObject { this }

fun PGbox.encode(): Encode = ToPgObject { this }

fun PGcircle.encode(): Encode = ToPgObject { this }

fun PGline.encode(): Encode = ToPgObject { this }

fun PGlseg.encode(): Encode = ToPgObject { this }

fun PGpath.encode(): Encode = ToPgObject { this }

fun PGpoint.encode(): Encode = ToPgObject { this }

fun PGpolygon.encode(): Encode = ToPgObject { this }

fun PGmoney.encode(): Encode = ToPgObject { this }
