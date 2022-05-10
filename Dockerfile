FROM openjdk:11
MAINTAINER midsizemango
ADD build/libs/elasticsearch-0.1.jar elasticsearch-0.1.jar
ENTRYPOINT ["java", "-jar", "/elasticsearch-0.1.jar"]