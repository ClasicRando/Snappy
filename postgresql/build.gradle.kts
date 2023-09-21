val kotlinReflectVersion: String by project
val kotlinTestVersion: String by project
val postgresqlJdbcVersion: String by project
val mockkVersion: String by project

dependencies {
    implementation(project(":core"))
    implementation(kotlin("reflect", version = kotlinReflectVersion))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("org.postgresql:postgresql:$postgresqlJdbcVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    testImplementation("io.mockk:mockk:$mockkVersion")
}
