plugins {
    java
}

group = "com.github.awelless"
version = "0.1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    version = JavaVersion.VERSION_1_8
}

dependencies {
    compileOnly("software.amazon.awssdk:s3:2.17.294")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.0")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
