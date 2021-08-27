FROM openjdk:11-slim-buster

RUN apt-get update
RUN apt-get install -y wget gnupg dos2unix

# install mongo-export
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian10-x86_64-100.5.0.deb && \
    apt install ./mongodb-database-tools-*-100.5.0.deb

# install mongosh
RUN wget -qO - https://www.mongodb.org/static/pgp/server-5.0.asc | apt-key add -
RUN echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/debian buster/mongodb-org/5.0 main" | tee /etc/apt/sources.list.d/mongodb-org-5.0.list

RUN apt-get update

RUN apt-get install -y mongodb-mongosh
