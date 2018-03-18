cd $1
# zookeeper-3.4.11/bin/zkServer.sh start
java -jar m3-server.jar $2 $3 $4 > "out$2.txt" 2>&1
