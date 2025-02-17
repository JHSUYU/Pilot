# Package the code
mvn clean package -DskipTests

The generated .jar will be Pilot-1.0-SNAPSHOT.jar in the target directory. Move the jar to /lib directory of apache project, then we could use the /lib