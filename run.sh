
#!/bin/sh

USER_HOME=${HOME};


java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Xmx8g -Xms8g -cp .:classes:target/classes:$CLASSPATH:/home/debs/debs-latest/lib/guava-13.0.1.jar:/home/debs/debs-latest/lib/log4j-1.2.17.jar:/home/debs/debs-latest/lib/disruptor-3.2.1.wso2v1.jar debs2015.ManagerWithFileWriteThread_Handler_VM  $1 $2
