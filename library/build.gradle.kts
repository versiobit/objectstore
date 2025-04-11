plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.vanniktech.mavenPublish)
}

group = "com.versiobit"
version = "0.1.0"

kotlin {
    jvm()
    js {
        nodejs()
    }

    sourceSets {
        val commonMain by getting {
            dependencies {
                implementation(libs.kotlinx.coroutines.core)
            }
        }
        val commonTest by getting {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotlinx.coroutines.test)
            }
        }
        val jvmMain by getting {
            dependencies {
                implementation("software.amazon.awssdk:s3:2.+")
            }
        }
    }
}

mavenPublishing {
    //publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)

    //signAllPublications()

    coordinates(group.toString(), "objectstore", version.toString())

//    pom {
//        name = "My library"
//        description = "A library."
//        inceptionYear = "2024"
//        url = "https://github.com/kotlin/multiplatform-library-template/"
//        licenses {
//            license {
//                name = "XXX"
//                url = "YYY"
//                distribution = "ZZZ"
//            }
//        }
//        developers {
//            developer {
//                id = "XXX"
//                name = "YYY"
//                url = "ZZZ"
//            }
//        }
//        scm {
//            url = "XXX"
//            connection = "YYY"
//            developerConnection = "ZZZ"
//        }
//    }
}
