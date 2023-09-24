plugins {
    kotlin("jvm")
}

version = "0.1"

allprojects {
    repositories {
        mavenCentral()
    }
    group = "org.snappy"
    version = "0.1"
}

subprojects {
    apply(plugin = "kotlin")

    kotlin {
        jvmToolchain(11)
    }

    val coroutineVersion: String by project
    val kotlinLoggingVersion: String by project
    val slfj4Version: String by project
    val logbackVersion: String by project
    val kotlinxSerializationVersion: String by project
    val classGraphVersion: String by project
    val junitVersion: String by project

    dependencies {
        implementation("ch.qos.logback:logback-classic:$logbackVersion")
        implementation("org.slf4j:slf4j-api:$slfj4Version")
        implementation("io.github.oshai:kotlin-logging-jvm:$kotlinLoggingVersion")
        // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core
        implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutineVersion")
        // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-core-jvm
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-core-jvm:$kotlinxSerializationVersion")
        // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-serialization-json-jvm
        implementation("org.jetbrains.kotlinx:kotlinx-serialization-json-jvm:$kotlinxSerializationVersion")
        // https://mvnrepository.com/artifact/io.github.classgraph/classgraph
        implementation("io.github.classgraph:classgraph:$classGraphVersion")

        testImplementation("org.junit.jupiter:junit-jupiter:$junitVersion")
        testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    }

    tasks.test {
        testLogging {
            setExceptionFormat("full")
            events = setOf(
                org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.SKIPPED,
                org.gradle.api.tasks.testing.logging.TestLogEvent.PASSED,
            )
            showStandardStreams = true
            afterSuite(KotlinClosure2<TestDescriptor,TestResult,Unit>({ descriptor, result ->
                if (descriptor.parent == null) {
                    println("\nTest Result: ${result.resultType}")
                    println("""
                    Test summary: ${result.testCount} tests, 
                    ${result.successfulTestCount} succeeded, 
                    ${result.failedTestCount} failed, 
                    ${result.skippedTestCount} skipped
                """.trimIndent().replace("\n", ""))
                }
            }))
        }
        useJUnitPlatform()
    }
}
