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

if [ $1 == "clean" ]; then
    clean
elif [ $1 == "install" ]; then
    install
elif [ $1 == "start" ]; then
    start
elif [ $1 == "stop" ]; then
    stop
fi
