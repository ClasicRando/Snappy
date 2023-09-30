package org.snappy.annotations

/**
 * Attach this annotation to properties that have another name in the [java.sql.ResultSet] returned
 * from a SQL query and auto mapping the column name to a property will save some SQL changes.
 */
@Target(AnnotationTarget.PROPERTY, AnnotationTarget.PROPERTY_SETTER)
annotation class Rename(val name: String)
