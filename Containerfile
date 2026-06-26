FROM docker.io/eclipse-temurin:21-jdk AS build

WORKDIR /app

RUN apt-get update && apt-get install -y curl gnupg && \
    mkdir -p /etc/apt/keyrings && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
      | gpg --dearmor > /etc/apt/keyrings/sbt.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/sbt.gpg] https://repo.scala-sbt.org/scalasbt/debian all main" \
      | tee /etc/apt/sources.list.d/sbt.list && \
    apt-get update && apt-get install -y sbt

COPY build.sbt .
COPY project/ project/

RUN sbt update

COPY src/ src/
RUN sbt assembly

FROM docker.io/eclipse-temurin:21-jre

WORKDIR /app

COPY --from=build /app/target/scala-2.13/personal-web-analytics.jar /app/app.jar

RUN mkdir -p /certs /data

VOLUME ["/certs", "/data"]

ENTRYPOINT ["java", "-jar", "/app/app.jar", "prod"]
