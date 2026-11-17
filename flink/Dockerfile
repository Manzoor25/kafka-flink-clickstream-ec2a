# syntax=docker/dockerfile:1
FROM maven:3.9.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build
COPY flink/pom.xml ./pom.xml
RUN mvn -B -q dependency:go-offline
COPY flink/src/main ./src/main
RUN mvn -B -q package -DskipTests

FROM flink:2.2.1-scala_2.12-java21
# Permanently activate built-in S3 Hadoop filesystem plugin for MinIO checkpoints & CLI savepoints
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop /opt/flink/usrlib /opt/flink/sql /opt/flink/schemas \
    && cp /opt/flink/opt/flink-s3-fs-hadoop-2.2.1.jar /opt/flink/plugins/s3-fs-hadoop/ \
    && chown -R flink:flink /opt/flink/plugins/s3-fs-hadoop /opt/flink/usrlib /opt/flink/sql /opt/flink/schemas

COPY --chown=flink:flink --from=builder /build/target/clickstream-flink-jobs-1.0.0.jar /opt/flink/usrlib/clickstream-flink-jobs.jar
COPY --chown=flink:flink flink/sql/ /opt/flink/sql/
COPY --chown=flink:flink schemas/ /opt/flink/schemas/
