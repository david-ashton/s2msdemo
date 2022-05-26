#!/bin/bash

export CLASSPATH=S2dbPersonQuery.jar
export JAVA_HOME=~/utils/jdk-16.0.1/
export PATH=$JAVA_HOME/bin:$PATH
export MAX_HEAP=8G
export MIN_HEAP=8G
export THREAD_STACK=1m
java -Xmx$MAX_HEAP -Xms$MIN_HEAP -Xss$THREAD_STACK -jar S2dbPersonQuery.jar
