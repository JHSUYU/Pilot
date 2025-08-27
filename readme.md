# Package the code
mvn clean package -DskipTests

The generated .jar will be Pilot-1.0-SNAPSHOT.jar in the target directory. Move the jar to /lib directory of apache project, then we could use the /lib


rm -rf /Users/lizhenyu/Desktop/Pilot/solr.log && scp ZhenyuLi@ms1132.utah.cloudlab.us:/opt/Solr/solr/server/logs/solr.log /Users/lizhenyu/Desktop/Pilot