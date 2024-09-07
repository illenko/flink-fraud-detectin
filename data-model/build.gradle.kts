val avroVersion: String by project

plugins {
    id("java")
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("maven-publish")
}

group = "com.illenko"
version = "1.0.0"

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("org.apache.avro:avro:$avroVersion")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
        }
    }
    repositories {
        mavenLocal()
    }
}
