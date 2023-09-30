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

/** [PgObjectDecoder] for a [PGInterval] */
class PgIntervalDecoder : PgObjectDecoder<PGInterval> {
    override fun decodePgObject(pgObject: PGobject): PGInterval? {
        if (pgObject is PGInterval) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGInterval::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGbox] */
class PgBoxDecoder : PgObjectDecoder<PGbox> {
    override fun decodePgObject(pgObject: PGobject): PGbox? {
        if (pgObject is PGbox) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGbox::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGcircle] */
class PgCircleDecoder : PgObjectDecoder<PGcircle> {
    override fun decodePgObject(pgObject: PGobject): PGcircle? {
        if (pgObject is PGcircle) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGcircle::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGline] */
class PgLineDecoder : PgObjectDecoder<PGline> {
    override fun decodePgObject(pgObject: PGobject): PGline? {
        if (pgObject is PGline) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGline::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGlseg] */
class PgLineSegmentDecoder : PgObjectDecoder<PGlseg> {
    override fun decodePgObject(pgObject: PGobject): PGlseg? {
        if (pgObject is PGlseg) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGlseg::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGpath] */
class PgPathDecoder : PgObjectDecoder<PGpath> {
    override fun decodePgObject(pgObject: PGobject): PGpath? {
        if (pgObject is PGpath) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGpath::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGpoint] */
class PgPointDecoder : PgObjectDecoder<PGpoint> {
    override fun decodePgObject(pgObject: PGobject): PGpoint? {
        if (pgObject is PGpoint) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGpoint::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGpolygon] */
class PgPolygonDecoder : PgObjectDecoder<PGpolygon> {
    override fun decodePgObject(pgObject: PGobject): PGpolygon? {
        if (pgObject is PGpolygon) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGpolygon::class, pgObject)
    }
}

/** [PgObjectDecoder] for a [PGmoney] */
class PgMoneyDecoder : PgObjectDecoder<PGmoney> {
    override fun decodePgObject(pgObject: PGobject): PGmoney? {
        if (pgObject is PGmoney) {
            return pgObject.takeIf { !it.isNull }
        }
        decodeError(PGmoney::class, pgObject)
    }
}
