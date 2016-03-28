#!/bin/bash

WORKERS=(helium-03 helium-04 helium-05)
MYSPACE="/import/helium-share/csgrad/zhangh15"

SPARK_PACKAGE="http://apache.mirror.gtcomm.net/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz"
SPARK_VERSION="spark-1.6.1-bin-hadoop2.6"

function clean {
    echo "Clean previous results...!"

    ssh helium-01 'rm -rf ' $MYSPACE/*

    for item in ${WORKERS[*]}; do
        ssh $item 'rm -rf /tmp/'
    done

    echo "Clean Done!"
}

function install {
    echo "Install On Workers..."

    ssh helium-01 wget ${SPARK_PACKAGE} -nv -O ${MYSPACE}/${SPARK_VERSION}.tgz
    ssh helium-01 tar xf ${MYSPACE}/${SPARK_VERSION}.tgz -C /${MYSPACE}/

    echo "Install Done!"
}

function start {
    echo "Starting Workers..."

    for item in ${WORKERS[*]}; do
        ssh $item ${MYSPACE}/${SPARK_VERSION}/sbin/start-slave.sh spark://DOMBA-03.cs.umanitoba.ca:7077
    done

    echo "Start Done!"
}

function stop {
    echo "Stopping..."

    for item in ${WORKERS[*]}; do
        ssh $item ${MYSPACE}/${SPARK_VERSION}//sbin/stop-slave.sh
    done

    echo "Stop Done!"
}

function run {
    APP=$1
    CMD="/usr/local/spark/bin/spark-submit "
    MASTER="spark://DOMBA-03.cs.umanitoba.ca:7077"
    #DB="/tmp/retail_lined.txt"
    DB="/tmp/retail.txt"
    MINSUP="0.05"
    CONF="--executor-memory 20G --conf spark.eventLog.enabled=true"

    echo $APP

    if [ $APP == "svt" ]; then
        ${CMD} --class "SVT" \
               --master ${MASTER} ${CONF} \
               $PWD/SVT/target/scala-2.10/svt_2.10-3.0.jar \
               ${MINSUP}
    elif [ $APP = "fp" ]; then
        ${CMD} --class org.apache.spark.examples.mllib.FPGrowthExample \
               --master ${MASTER} ${CONF} \
               /usr/local/spark/lib/spark-examples-1.6.1-hadoop2.6.0.jar \
               --minSupport ${MINSUP} \
               ${DB}
    fi
}

if [ $1 == "clean" ]; then
    clean
elif [ $1 == "install" ]; then
    install
elif [ $1 == "start" ]; then
    start
elif [ $1 == "stop" ]; then
    stop
elif [ $1 == "run" ]; then
    run $2
fi
