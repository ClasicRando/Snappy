pluginManagement {
    val kotlinVersion: String by settings
    val kotlinxSerializationPluginVersion: String by settings
    val kspVersion: String by settings
    plugins {
        kotlin("jvm") version kotlinVersion
        kotlin("plugin.serialization") version kotlinxSerializationPluginVersion
        id("com.google.devtools.ksp") version kspVersion
    }
}

rootProject.name = "snappy"

include(":core")
include("benchmark")
include("postgresql")
include("mssql")
include("ksp")
