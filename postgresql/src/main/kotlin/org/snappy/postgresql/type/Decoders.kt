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
import org.postgresql.util.PGobject
import org.snappy.decodeError
import kotlin.reflect.KClass

class PgIntervalDecoder : PgObjectDecoder<PGInterval> {
    override fun decodePgObject(pgObject: PGobject): PGInterval {
        if (pgObject is PGInterval) {
            return pgObject
        }
        decodeError(PGInterval::class, pgObject)
    }
}

class PgBoxDecoder : PgObjectDecoder<PGbox> {
    override fun decodePgObject(pgObject: PGobject): PGbox {
        if (pgObject is PGbox) {
            return pgObject
        }
        decodeError(PGbox::class, pgObject)
    }
}

class PgCircleDecoder : PgObjectDecoder<PGcircle> {
    override fun decodePgObject(pgObject: PGobject): PGcircle {
        if (pgObject is PGcircle) {
            return pgObject
        }
        decodeError(PGcircle::class, pgObject)
    }
}

class PgLineDecoder : PgObjectDecoder<PGline> {
    override fun decodePgObject(pgObject: PGobject): PGline {
        if (pgObject is PGline) {
            return pgObject
        }
        decodeError(PGline::class, pgObject)
    }
}

class PgLineSegmentDecoder : PgObjectDecoder<PGlseg> {
    override fun decodePgObject(pgObject: PGobject): PGlseg {
        if (pgObject is PGlseg) {
            return pgObject
        }
        decodeError(PGlseg::class, pgObject)
    }
}

class PgPathDecoder : PgObjectDecoder<PGpath> {
    override fun decodePgObject(pgObject: PGobject): PGpath {
        if (pgObject is PGpath) {
            return pgObject
        }
        decodeError(PGpath::class, pgObject)
    }
}

class PgPointDecoder : PgObjectDecoder<PGpoint> {
    override fun decodePgObject(pgObject: PGobject): PGpoint {
        if (pgObject is PGpoint) {
            return pgObject
        }
        decodeError(PGpoint::class, pgObject)
    }
}

class PgPolygonDecoder : PgObjectDecoder<PGpolygon> {
    override fun decodePgObject(pgObject: PGobject): PGpolygon {
        if (pgObject is PGpolygon) {
            return pgObject
        }
        decodeError(PGpolygon::class, pgObject)
    }
}

class PgMoneyDecoder : PgObjectDecoder<PGmoney> {
    override fun decodePgObject(pgObject: PGobject): PGmoney {
        if (pgObject is PGmoney) {
            return pgObject
        }
        decodeError(PGmoney::class, pgObject)
    }
}
