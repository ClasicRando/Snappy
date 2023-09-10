dependencies {
    implementation(kotlin("reflect"))
    // https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.3")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.8.1")
    testImplementation("io.mockk:mockk:1.13.7")
    // https://mvnrepository.com/artifact/io.github.classgraph/classgraph
    implementation("io.github.classgraph:classgraph:4.8.162")
    // https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc
    testImplementation("org.xerial:sqlite-jdbc:3.43.0.0")
}
