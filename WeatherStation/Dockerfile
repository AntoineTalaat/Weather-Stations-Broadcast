## Base image with Maven installed
#FROM openjdk:17-jdk-slim-buster AS build
#
## Set the working directory in the container
#WORKDIR /app
#
## Copy the pom.xml file to the container
#COPY pom.xml .
#
## Set the environment variable for the pom.xml file
#ENV MAVEN_POM=/app/pom.xml
#
## Download dependencies and build the application
#RUN mvn dependency:go-offline -B
#
## Final image with only JRE and the built JAR
#FROM openjdk:11-jre-slim
#
## Set the working directory in the container
#WORKDIR /app
#
## Copy the built JAR from the previous stage
#COPY ./target/WeatherStation-0.0.1-SNAPSHOT.jar ./WeatherStation-0.0.1-SNAPSHOT.jar
#
## Set the environment variable for the city
#
## Run the Spring Boot application when the container starts
#ENTRYPOINT ["java", "-jar", "WeatherStation-0.0.1-SNAPSHOT.jar"]
#
#
#
#ENV CITY=your_default_city

FROM openjdk:17-jdk-slim-buster
LABEL maintainer="OmarKhairat"
ARG KAFKA_BOOTSTRAP_SERVERS=192.168.1.12:9092
ENV KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}
ARG CITY=2
ENV CITY=your_default_city
ADD target/WeatherStation-0.0.1-SNAPSHOT.jar WeatherStation-0.0.1-SNAPSHOT.jar
ENTRYPOINT ["java", "-jar", "WeatherStation-0.0.1-SNAPSHOT.jar"]