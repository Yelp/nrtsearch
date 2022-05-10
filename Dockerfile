FROM azul/zulu-openjdk-debian:14

ENV DEBIAN_FRONTEND noninteractive

RUN mkdir /usr/app/
COPY . /user/app/ 
WORKDIR /user/app/ 

RUN ./gradlew clean installDist 
