package org.snappy.benchmarks

import java.sql.DriverManager


private const val missingEnvironmentVariableMessage = "To run MultiResultTest " +
        "the environment variable SNAPPY_MSSQL_CONNECTION_STRING must be available"

private val connectionString = System.getenv("SNAPPY_MSSQL_CONNECTION_STRING")
    ?: throw IllegalStateException(missingEnvironmentVariableMessage)

fun getConnection() = DriverManager.getConnection(connectionString)
