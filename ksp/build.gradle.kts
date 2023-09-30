plugins {
    kotlin("plugin.serialization")
}

val kspVersion: String by project

dependencies {
    api(project(":core"))
    api(project(":postgresql"))
    api(project(":mssql"))
    // https://mvnrepository.com/artifact/org.postgresql/postgresql
    implementation("com.google.devtools.ksp:symbol-processing-api:$kspVersion")
}
