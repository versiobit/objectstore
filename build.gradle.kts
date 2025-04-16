import com.vanniktech.maven.publish.SonatypeHost

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    id("com.vanniktech.maven.publish") version "0.29.0" 
}

group = "com.versiobit"
version = "0.1.3"

kotlin {
    jvm()
    js()

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
    publishToMavenCentral(SonatypeHost.CENTRAL_PORTAL)
    
    signAllPublications()
    
    coordinates(group.toString(), name.toString(), version.toString())
    
    pom { 
        name = "Object Store"
        description = "A generic abstraction for object-based storage e.g. local filesystems, cloud storage solutions, or other data stores."
        inceptionYear = "2025"
        url = "https://github.com/versiobit/objectstore"
        licenses {
            license {
                name = "GNU GENERAL PUBLIC LICENSE Version 3"
                url = "https://www.gnu.org/licenses/gpl-3.0.html"
                distribution = "https://www.gnu.org/licenses/gpl-3.0.html"
            }
        }
        developers {
            developer {
                id = "versiobit"
                name = "Versiobit"
                url = "https://github.com/versiobit/"
            }
        }
        scm {
            url = "https://github.com/versiobit/objectstore"
            connection = "scm:git:git://github.com/versiobit/objectstore.git"
            developerConnection = "scm:git:ssh://git@github.com/versiobit/objectstore.git"
        }
    }
}