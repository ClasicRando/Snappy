package org.snappy.postgresql.json

import java.sql.DriverManager

data class PgJson(val value: String) {

}

fun main() {
    DriverManager.getConnection(System.getenv("SNAPPY_PG_CONNECTION_STRING")).use { c ->
        c.createStatement().use {
            it.executeQuery("select '{}'::jsonb").use { rs ->
                rs.next()
                val value = rs.getString(1)
                println("Value: $value")
            }
        }
    }
}
