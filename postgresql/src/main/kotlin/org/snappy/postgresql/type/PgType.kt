package org.snappy.postgresql.type

/**
 * Annotation telling KSP and other reflection based code this class represents a type in a postgres
 * database. That type can either be a composite or enum type with the specified [name]
 */
@Target(AnnotationTarget.CLASS)
annotation class PgType(val name: String)
