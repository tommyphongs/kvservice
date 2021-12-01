FROM openjdk:14
COPY ./target/kvservice-jar-with-dependencies.jar /usr/app/app.jar
COPY src/main/resources/docker_log4j.properties /usr/app/log4j.properties
ENV JAVA_OPTS="-Xms4096m -Xmx20480m"
RUN mkdir -p  /data
RUN mkdir -p /data/log
RUN chmod -R 777 /data
ENV DATA_DIR=/data
ENV HTTP_PORT=9898
ENV GRPC_PORT=9999
ENV LOG_DIR=/data/log
ENV MODE=MASTER
ENV LOG4J_CONF_FILE=/usr/app/log4j.properties
WORKDIR /usr/app
USER 10001
ENTRYPOINT ["java", "-jar", "app.jar"]