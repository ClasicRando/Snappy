plugins {
    id("com.google.devtools.ksp")
}

val kotlinTestVersion: String by project
val mockkVersion: String by project
val mssqlJdbcVersion: String by project
val postgresqlJdbcVersion: String by project
val sqliteJdbcVersion: String by project

dependencies {
    implementation(project(":core"))
    ksp(project(":core"))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql:postgresql:$postgresqlJdbcVersion")
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    implementation("com.microsoft.sqlserver:mssql-jdbc:$mssqlJdbcVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    testImplementation("io.mockk:mockk:$mockkVersion")
    // https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
    testImplementation("org.xerial:sqlite-jdbc:$sqliteJdbcVersion")
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    testImplementation("com.microsoft.sqlserver:mssql-jdbc:$mssqlJdbcVersion")
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    testImplementation("org.postgresql:postgresql:$postgresqlJdbcVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
}
