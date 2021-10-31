plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

fun toMavenCoordinates(notation: Provider<PluginDependency>): Provider<String> {
    return notation.map { "${it.pluginId}:${it.pluginId}.gradle.plugin:${it.version.displayName}" }
}

dependencies {
    implementation(toMavenCoordinates(libs.plugins.kotlin.jvm))
    implementation(toMavenCoordinates(libs.plugins.lombok))
    implementation(gradleApi())
    implementation(embeddedKotlin("stdlib-jdk8"))
}
