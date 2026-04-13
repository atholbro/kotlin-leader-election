import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    `java-library`
    `maven-publish`
    signing
    jacoco

    alias(libs.plugins.kotlin.jvm)

    alias(libs.plugins.nmcp)
    alias(libs.plugins.nmcp.aggregation)
}

dependencies {
    api(libs.k8s.client)
    api(libs.kotlin.coroutines.core)

    implementation(libs.kotlinLogging)

    testImplementation(libs.mockk)
    nmcpAggregation(project(":kotlin-leader-election"))
}

java {
    withJavadocJar()
    withSourcesJar()
}

jacoco {
    toolVersion = libs.versions.jacoco.get()
}

tasks {
    jar {
        archiveBaseName.set("kotlin-leader-election")
    }

    withType<JavaCompile>().configureEach {
        options.release.set(17)
    }

    withType<KotlinCompile>().configureEach {
        compilerOptions {
            freeCompilerArgs.add("-opt-in=net.aholbrook.kotlinleaderelection.InternalApi")
        }
    }


    withType<AbstractPublishToMaven>().configureEach {
        dependsOn(rootProject.tasks.named("check"))
    }

    withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }

    withType<Javadoc>().configureEach {
        options {
            (this as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
        }
    }

    jacocoTestReport {
        dependsOn(test)

        reports {
            xml.required = true
            csv.required = true
            html.required = true
        }
    }
}

publishing {
    publications {
        group = "net.aholbrook.kotlin-leader-election"
        version = System.getenv("VERSION") ?: ""

        create<MavenPublication>("maven") {
            artifactId = "kotlin-leader-election"
            from(components["java"])

            pom {
                name.set("kotlin-leader-election")
                description.set("Kotlin Leader Election via Kubernetes Leases.")
                url.set("https://github.com/atholbro/kotlin-leader-election")

                licenses {
                    license {
                        name.set("The MIT License (MIT)")
                        url.set("https://opensource.org/licenses/MIT")
                    }
                }

                developers {
                    developer {
                        id.set("aholbrook")
                        name.set("Andrew Holbrook")
                        email.set("atholbro@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git@github.com:atholbro/kotlin-leader-election.git")
                    developerConnection.set("scm:git:git@github.com:atholbro/kotlin-leader-election.git")
                    url.set("https://github.com/atholbro/kotlin-leader-election")
                }
            }
        }
    }

    repositories {
        maven {
            url = if (version.toString().endsWith("SNAPSHOT")) {
                uri("https://central.sonatype.com/repository/maven-snapshots/")
            } else {
                uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            }

            credentials {
                username = System.getenv("PUBLISH_USER")
                password = System.getenv("PUBLISH_PASS")
            }
        }
    }
}

signing {
    useInMemoryPgpKeys(
        System.getenv("GPG_KEY"),
        System.getenv("GPG_PASS"),
    )
    sign(publishing.publications)
}

nmcpAggregation {
    centralPortal {
        username = System.getenv("PUBLISH_USER")
        password = System.getenv("PUBLISH_PASS")

        publishingType = "AUTOMATIC"
    }
}
