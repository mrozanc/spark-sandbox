import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("spark-sandbox.java-conventions")
    kotlin("jvm")
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xjsr305=strict")
        jvmTarget = "11"
    }
}
