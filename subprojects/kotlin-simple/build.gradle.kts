plugins {
    id("spark-sandbox.kotlin-conventions")
}

configurations {
    all {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
}

dependencies {
    implementation(libs.kotlinx.spark.api)
    implementation(libs.bundles.spark.main)
    implementation(libs.bundles.kotlin)
    runtimeOnly(libs.bundles.log4j2.runtime)

    testImplementation(libs.bundles.kotest)
}

tasks {
    named<Test>("test") {
        systemProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
    }
}
