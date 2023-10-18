plugins {
    kotlin("plugin.serialization")
    `maven-publish`
}

val kotlinReflectVersion: String by project
val kotlinTestVersion: String by project
val mockkVersion: String by project
val sqliteJdbcVersion: String by project
val mssqlJdbcVersion: String by project
val kspVersion: String by project
val postgresqlJdbcVersion: String by project

dependencies {
    implementation(kotlin("reflect", version = kotlinReflectVersion))
    implementation("com.google.devtools.ksp:symbol-processing-api:$kspVersion")
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql:postgresql:$postgresqlJdbcVersion")
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    implementation("com.microsoft.sqlserver:mssql-jdbc:$mssqlJdbcVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    // https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
    testImplementation("org.xerial:sqlite-jdbc:$sqliteJdbcVersion")
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    testImplementation("com.microsoft.sqlserver:mssql-jdbc:$mssqlJdbcVersion")
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    testImplementation("org.postgresql:postgresql:$postgresqlJdbcVersion")
    testImplementation("io.mockk:mockk:$mockkVersion")
}

publishing {
    repositories {
        mavenLocal()
    }
    publications {
        create<MavenPublication>("mavenJava") {
            groupId = "com.github.snappy"
            artifactId = "snappy"
            version = "0.1"
            pom {
                name = "Snappy"
                description = "Kotlin library providing similar functionality to Dapper for the JDBC"
                url = "https://github.com/ClasicRando/snappy"
            }
            from(components["kotlin"])
        }
    }
}
