FROM openjdk:11-slim-buster

RUN apt-get update
RUN apt-get install -y mongo-tools
