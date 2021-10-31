plugins {
    id("spark-sandbox.java-conventions")
}

configurations {
    all {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
        exclude(group = "log4j", module = "log4j")
    }
}

dependencies {
    implementation(libs.bundles.spark2.main)
    implementation(libs.guava)

    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.bundles.log4j2.runtime)
}

tasks {
    named<Test>("test") {
        systemProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager")
    }
}
