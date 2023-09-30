val kotlinReflectVersion: String by project
val kotlinTestVersion: String by project
val mssqlJdbcVersion: String by project
val mockkVersion: String by project

dependencies {
    api(project(":core"))
    api(kotlin("reflect", version = kotlinReflectVersion))
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    api("com.microsoft.sqlserver:mssql-jdbc:$mssqlJdbcVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    testImplementation("io.mockk:mockk:$mockkVersion")
}
