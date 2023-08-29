# Use the Maven base image
FROM maven:3.8.3-openjdk-17

# Set the working directory
WORKDIR /app

# Copy necessary files for build
COPY src src
COPY pom.xml .

# Build the JAR file
RUN mvn clean install -DskipTests

# Copy the JAR file
RUN cp ./target/ratings-calculation-service*.jar ./app.jar

# Entrypoint
ENTRYPOINT ["java", "-Dspring.profiles.active=@env@", "-jar", "./app.jar"]