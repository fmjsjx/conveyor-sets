plugins {
    java
    id("org.springframework.boot") version "2.6.3"
    distribution
}

apply(plugin = "io.spring.dependency-management")

group = "com.github.fmjsjx"
version = "1.0.0"
description = "Conveyor Sets"

repositories {
    maven {
        url = uri("https://mirrors.cloud.tencent.com/nexus/repository/maven-public/")
    }
    mavenCentral()
}

configurations {
	compileOnly.extendsFrom(configurations.annotationProcessor.get())
	"implementation" {
	    // using log4j2 must exclude logback
        exclude(module = "spring-boot-starter-logging")
    }
}

dependencies {
    implementation(platform("com.github.fmjsjx:libnetty-bom:2.4.2"))
    implementation(platform("com.github.fmjsjx:libcommon-bom:2.6.1"))
    implementation(platform("com.github.fmjsjx:myboot-bom:1.1.9"))

    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")
    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-log4j2")
    implementation("com.lmax:disruptor:3.4.4")
    implementation("mysql:mysql-connector-java")
    implementation("com.github.fmjsjx:libcommon-collection")
    implementation("com.zaxxer:HikariCP")
    implementation("com.github.fmjsjx:libcommon-util")
    implementation("com.github.fmjsjx:libcommon-json-jackson2")
    implementation("com.github.fmjsjx:libcommon-json-jsoniter")
    implementation("org.javassist:javassist:3.28.0-GA")
    implementation("com.github.fmjsjx:libcommon-yaml")
    implementation("com.github.fmjsjx:libcommon-redis")
    implementation("com.github.fmjsjx:libnetty-transport")
    implementation("com.github.fmjsjx:libnetty-resp3")
    implementation("io.lettuce:lettuce-core")
    implementation(group = "io.netty", name = "netty-tcnative-boringssl-static", classifier = "linux-x86_64")
    implementation(group = "io.netty", name = "netty-transport-native-epoll", classifier = "linux-x86_64")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
    compileOnly("org.springframework.boot:spring-boot-configuration-processor")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

distributions {
    main {
        contents {
            from("src/main/bin") {
                filesMatching("*.sh") {
                    setMode(0b111101101)
                }
                filesNotMatching("*.sh") {
                    setMode(0b110100100)
                }
            }
            into("conf/") {
                from("src/main/conf")
                setDirMode(0b111101101)
                setFileMode(0b110100100)
            }
            from(tasks.bootJar) {
                include("${project.name}-${project.version}.jar")
            }
        }
    }
}

tasks.compileJava {
    options.encoding = "UTF-8"
    options.release.set(17)
}

tasks.getByName<ProcessResources>("processResources") {
    // Automatic Property Expansion with Maven Compatible Solution
    filesMatching("application*.yml") {
        val projectInfo = mapOf("project.artifactId" to "${project.name}",
                                "project.groupId"    to "${project.group}",
                                "project.name"       to "${project.description}",
                                "project.version"    to "${project.version}")
        filter(org.apache.tools.ant.filters.ReplaceTokens::class, "tokens" to projectInfo)
    }
}

tasks.test {
    // Use junit platform for unit tests.
    useJUnitPlatform()
}

tasks.jar {
    // Disable plain Jar for Sprint Boot
    enabled = false
}

tasks.javadoc {
    enabled = false
}

tasks.distZip {
    enabled = false
}

tasks.distTar {
    compression = Compression.GZIP
    archiveExtension.set("tar.gz")
    doLast {
        file("${archiveFile}").renameTo(file("${destinationDirectory}/${project.name}-${project.version}-bin.tar.gz"))
    }
}
