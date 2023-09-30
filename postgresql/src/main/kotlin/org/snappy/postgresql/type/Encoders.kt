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

/** Use this [PGInterval] as the object passed to a [ToPgObject] encoder */
fun PGInterval.encode(): Encode = ToPgObject { this }

/** Use this [PGbox] as the object passed to a [ToPgObject] encoder */
fun PGbox.encode(): Encode = ToPgObject { this }

/** Use this [PGcircle] as the object passed to a [ToPgObject] encoder */
fun PGcircle.encode(): Encode = ToPgObject { this }

/** Use this [PGline] as the object passed to a [ToPgObject] encoder */
fun PGline.encode(): Encode = ToPgObject { this }

/** Use this [PGlseg] as the object passed to a [ToPgObject] encoder */
fun PGlseg.encode(): Encode = ToPgObject { this }

/** Use this [PGpath] as the object passed to a [ToPgObject] encoder */
fun PGpath.encode(): Encode = ToPgObject { this }

/** Use this [PGpoint] as the object passed to a [ToPgObject] encoder */
fun PGpoint.encode(): Encode = ToPgObject { this }

/** Use this [PGpolygon] as the object passed to a [ToPgObject] encoder */
fun PGpolygon.encode(): Encode = ToPgObject { this }

/** Use this [PGmoney] as the object passed to a [ToPgObject] encoder */
fun PGmoney.encode(): Encode = ToPgObject { this }
