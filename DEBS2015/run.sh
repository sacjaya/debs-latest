
#!/bin/sh

USER_HOME=${HOME};


java -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Xmx8g -Xms8g -cp .:target/classes:$CLASSPATH:$USER_HOME/.m2/repository/com/google/guava/guava/13.0.1/guava-13.0.1.jar:$USER_HOME//.m2/repository/log4j/log4j/1.2.14/log4j-1.2.14.jar:$USER_HOME/.m2/repository/com/lmax/disruptor/3.2.1/disruptor-3.2.1.jar debs2015.ManagerWithFileWriteThread_Handler_VM $1 $2
