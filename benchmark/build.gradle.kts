plugins {
    id("me.champeau.jmh") version ("0.7.1")
    id("com.google.devtools.ksp")
}

dependencies {
    implementation(project(":core"))
    ksp(project(":core"))
    // https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
    testImplementation("com.microsoft.sqlserver:mssql-jdbc:12.4.1.jre11")
    // https://mvnrepository.com/artifact/org.hibernate.orm/hibernate-core
    testImplementation("org.hibernate.orm:hibernate-core:6.3.0.Final")
    // https://mvnrepository.com/artifact/org.jetbrains.kotlin.plugin.jpa/org.jetbrains.kotlin.plugin.jpa.gradle.plugin
    implementation("org.jetbrains.kotlin.plugin.jpa:org.jetbrains.kotlin.plugin.jpa.gradle.plugin:1.9.10")
}

jmh {
    dependencies {
        jmhImplementation("org.openjdk.jmh:jmh-core:1.36")
        jmhImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.36")
        jmhAnnotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.36")
    }
}
