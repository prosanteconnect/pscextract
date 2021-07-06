FROM openjdk:11-slim-buster

RUN apt-get update
RUN apt-get install -y mongo-tools wget gnupg2 dos2unix

RUN echo "deb [ trusted=yes ] https://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list

RUN apt-get update

RUN apt-get install -y mongodb-org-shell
