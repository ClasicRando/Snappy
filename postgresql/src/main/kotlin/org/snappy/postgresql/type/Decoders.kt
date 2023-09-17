package org.snappy.postgresql.type

import org.postgresql.geometric.PGbox
import org.postgresql.geometric.PGcircle
import org.postgresql.geometric.PGline
import org.postgresql.geometric.PGlseg
import org.postgresql.geometric.PGpath
import org.postgresql.geometric.PGpoint
import org.postgresql.geometric.PGpolygon
import org.postgresql.util.PGInterval
import org.postgresql.util.PGobject
import org.snappy.decodeError
import kotlin.reflect.KClass

class PgIntervalDecoder : PgObjectDecoder<PGInterval> {
    override val decodeClass: KClass<PGInterval> = PGInterval::class

    override fun decodePgObject(pgObject: PGobject): PGInterval {
        if (pgObject is PGInterval) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgBoxDecoder : PgObjectDecoder<PGbox> {
    override val decodeClass: KClass<PGbox> = PGbox::class

    override fun decodePgObject(pgObject: PGobject): PGbox {
        if (pgObject is PGbox) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgCircleDecoder : PgObjectDecoder<PGcircle> {
    override val decodeClass: KClass<PGcircle> = PGcircle::class

    override fun decodePgObject(pgObject: PGobject): PGcircle {
        if (pgObject is PGcircle) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgLineDecoder : PgObjectDecoder<PGline> {
    override val decodeClass: KClass<PGline> = PGline::class

    override fun decodePgObject(pgObject: PGobject): PGline {
        if (pgObject is PGline) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgLineSegmentDecoder : PgObjectDecoder<PGlseg> {
    override val decodeClass: KClass<PGlseg> = PGlseg::class

    override fun decodePgObject(pgObject: PGobject): PGlseg {
        if (pgObject is PGlseg) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgPathDecoder : PgObjectDecoder<PGpath> {
    override val decodeClass: KClass<PGpath> = PGpath::class

    override fun decodePgObject(pgObject: PGobject): PGpath {
        if (pgObject is PGpath) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgPointDecoder : PgObjectDecoder<PGpoint> {
    override val decodeClass: KClass<PGpoint> = PGpoint::class

    override fun decodePgObject(pgObject: PGobject): PGpoint {
        if (pgObject is PGpoint) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}

class PgPolygonDecoder : PgObjectDecoder<PGpolygon> {
    override val decodeClass: KClass<PGpolygon> = PGpolygon::class

    override fun decodePgObject(pgObject: PGobject): PGpolygon {
        if (pgObject is PGpolygon) {
            return pgObject
        }
        decodeError(decodeClass, pgObject)
    }
}
