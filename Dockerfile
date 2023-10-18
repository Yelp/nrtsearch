FROM debian:12

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && \
    apt-get install -y wget gnupg2 software-properties-common

RUN wget -O- https://apt.corretto.aws/corretto.key | apt-key add -
RUN add-apt-repository -y 'deb https://apt.corretto.aws stable main'
# For some reason, needs to be run again for the repo to be usable
RUN add-apt-repository -y 'deb https://apt.corretto.aws stable main'
RUN apt-get update && \
    apt-get install -y java-17-amazon-corretto-jdk

RUN mkdir /usr/app/
COPY . /user/app/ 
WORKDIR /user/app/ 

RUN ./gradlew clean installDist 
