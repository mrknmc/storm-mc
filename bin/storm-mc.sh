#!/usr/bin/env bash

# good standards
: ${JAVA_CMD:=$JAVA_HOME/bin/java}
: ${STORM_DIR:=$HOME/storm-mc}
: ${CONFFILE=""}
: ${USER_CONF_DIR:=$HOME/.storm}

# set memory to at least 1.7G
: ${JVM_OPTS:="-Xmx1700M"}

CLASSPATH="$STORM_DIR/lib/*:$1:$STORM_DIR/conf:$STORM_DIR/bin"

java $JVM_OPTS -Dstorm.home=$STORM_DIR -Dstorm.conf.file=$CONFFILE -cp $CLASSPATH -Dstorm.jar=$1 $2 local $3