import io.freefair.gradle.plugins.lombok.LombokExtension

plugins {
    `java-library`
    id("io.freefair.lombok")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}

lombok {
    version.set(project.properties.getOrDefault("lombokVersion", LombokExtension.LOMBOK_VERSION).toString())
}

tasks.withType<Test> {
    useJUnitPlatform()
}
