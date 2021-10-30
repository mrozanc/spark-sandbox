enableFeaturePreview("VERSION_CATALOGS")

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

rootProject.name = "spark-sandbox"

file("$rootDir/subprojects")
    .listFiles { file -> file.isDirectory }
    ?.forEach { subProjectDir ->
        val subProjectName = ":${rootProject.name}-${subProjectDir.name}"
        include(subProjectName)
        project(subProjectName).projectDir = subProjectDir
    }
