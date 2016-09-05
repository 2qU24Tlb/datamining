#!/bin/bash

WORKERS=(helium-03 helium-04 helium-05)
MYSPACE="/import/helium-share/csgrad/zhangh15"

function clean {
    echo "Clean previous results...!"

    ssh helium-01 'rm -rf ' $MYSPACE/*

    for item in ${WORKERS[*]}; do
        ssh $item 'rm -rf /tmp/'
    done

    echo "Clean Done!"
}

function start {
    echo "Starting Master..."
    /usr/local/spark/sbin/start-master.sh &&

    echo "Starting Workers..."

    for item in ${WORKERS[*]}; do
        ssh $item ${MYSPACE}/spark/sbin/start-slave.sh spark://DOMBA-03.cs.umanitoba.ca:7077
    done

    echo "Start Done!"
}

function stop {
    echo "Stopping..."

    for item in ${WORKERS[*]}; do
        ssh $item ${MYSPACE}/spark/sbin/stop-slave.sh
    done

    /usr/local/spark/sbin/stop-master.sh &&

    echo "Stop Done!"
}

function run {
    APP=$1
    CMD="/usr/local/spark/bin/spark-submit "
    MASTER="spark://DOMBA-03.cs.umanitoba.ca:7077"
    DB="file:/tmp/retail.txt"
    MINSUP="0.5"
    #CONF="--executor-memory 20G --conf spark.eventLog.enabled=true"
    CONF="--conf spark.eventLog.enabled=true"

    if [ $APP == "svt" ]; then
        echo "running SVT..."
        ${CMD} --class SVTTest \
               --master ${MASTER} ${CONF} \
               $PWD/SVT/target/scala-2.10/svt_2.10-3.0.jar \
               ${MINSUP}
    elif [ $APP == "pfp" ]; then
        echo "running PFP..."
        ${CMD} --class FPGrowthExample \
               --master ${MASTER} ${CONF} \
               $PWD/FPGrowth/target/scala-2.10/fpgrowthexample_2.10-1.0.jar \
               ${MINSUP}
    elif [ $APP == "peclat" ]; then
        ${CMD} --class Peclat \
               --master ${MASTER} ${CONF} \
               $PWD/Peclat/target/scala-2.10/peclat_2.10-1.0.jar \
               ${MINSUP}
    elif [ $APP == "yafim" ]; then
        ${CMD} --class YAFIMTest \
               --master ${MASTER} ${CONF} \
               $PWD/YAFIM/target/scala-2.11/yafim_2.11-1.0.jar \
               ${DB} ${MINSUP}
    fi
}

function pack {
    APP=$1
    echo "compiling" $APP

    if [ $APP == "svt" ]; then
        cd $PWD/SVT; sbt package
    elif [ $APP == "pfp" ]; then
        cd $PWD/FPGrowth; sbt package	
    elif [ $APP == "peclat" ]; then
        cd $PWD/Peclat; sbt package
    elif [ $APP == "yafim" ]; then
        cd $PWD/YAFIM; sbt package
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
elif [ $1 == "pack" ]; then
    pack $2
fi
