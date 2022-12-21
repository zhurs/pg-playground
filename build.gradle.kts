import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.20"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("test"))
    implementation("org.junit.jupiter:junit-jupiter:5.8.1")
    implementation("org.testcontainers:testcontainers:1.17.6")
    implementation("org.testcontainers:junit-jupiter:1.17.6")
    implementation("org.testcontainers:postgresql:1.17.6")
    implementation("org.jetbrains.exposed:exposed-core:0.40.1")
    implementation("org.jetbrains.exposed:exposed-jdbc:0.40.1")
    implementation("org.postgresql:postgresql:42.5.1")
    implementation("org.slf4j:slf4j-simple:2.0.6")
    implementation("com.zaxxer:HikariCP:5.0.1")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    mainClass.set("MainKt")
}