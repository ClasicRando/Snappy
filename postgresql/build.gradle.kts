plugins {
    kotlin("plugin.serialization")
}

val kotlinReflectVersion: String by project
val kotlinTestVersion: String by project
val postgresqlJdbcVersion: String by project
val mockkVersion: String by project
val kspVersion: String by project

dependencies {
    api(project(":core"))
    api(kotlin("reflect", version = kotlinReflectVersion))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    api("org.postgresql:postgresql:$postgresqlJdbcVersion")
    implementation("com.google.devtools.ksp:symbol-processing-api:$kspVersion")

    testImplementation(kotlin("test", version = kotlinTestVersion))
    testImplementation("io.mockk:mockk:$mockkVersion")
}

sourceSets.main {
    java.srcDir("src/main/kotlin")
}
