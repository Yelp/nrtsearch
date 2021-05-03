FROM adoptopenjdk/openjdk14 as jdk14
WORKDIR /build
RUN apt-get update && apt-get upgrade -y

FROM jdk14 as builder
# install gradle
RUN apt-get install wget unzip -y
RUN wget https://services.gradle.org/distributions/gradle-6.4.1-bin.zip -P /tmp
RUN unzip -d /opt/gradle /tmp/gradle-*.zip
ENV GRADLE_HOME /opt/gradle/gradle-6.4.1
ENV PATH ${GRADLE_HOME}/bin:${PATH}

COPY ./src /build/src
COPY ./clientlib /build/clientlib
COPY ./gradle ./gradlew ./*.gradle /build/

# install appropriate gradle wrapper
RUN gradle wrapper

# build
RUN ./gradlew clean && ./gradlew installDist

FROM adoptopenjdk/openjdk14 as server
WORKDIR /app
COPY --from=builder /build/build/install/nrtsearch /app
COPY ./plugin_defs/ /root/lucene/server/plugins/
COPY --from=builder /build/build/libs/* /root/lucene/server/plugins/chaseplugin/
COPY ./chase_server_config.yaml /root/lucene/server/config.yaml
RUN apt-get update && apt-get upgrade -y
CMD /app/bin/lucene-server /root/lucene/server/config.yaml