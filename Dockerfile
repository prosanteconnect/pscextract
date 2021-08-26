FROM openjdk:11-slim-buster

RUN apt-get update
RUN apt-get install -y mongo-tools wget gnupg dos2unix

RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -

RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list

RUN apt-get update

RUN apt-get install -y mongodb-mongosh
