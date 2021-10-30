plugins {
    id("spark-sandbox.java-conventions")
}

dependencies {
    implementation(libs.bundles.spark.main)
    implementation(libs.guava)

    testImplementation(libs.assertj.core)
    testImplementation(libs.junit.jupiter)
}
