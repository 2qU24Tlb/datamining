#!/usr/bin/bash

SPARK="http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.6.tgz"
HADOOP="http://apache.sunsite.ualberta.ca/hadoop/common/hadoop-2.6.2/hadoop-2.6.2.tar.gz"
WORKERS=(helium-01 helium-02 helium-03 helium-04 helium-05)
WORKSPACE="/import/helium-share/csgrad/zhangh15"
HADOOP_PREFIX="${WORKSPACE}/${HADOOP_VERSION}"

function check {
    echo "Checking the files.."
    if [ -f "${WORKSPACE}/clients.tar.gz" ] ;then
        echo "find configuration..."
    else
        echo "can't find configuration"
        exit
    fi

    echo "Check done!"
}

function clean {
    echo "Clean previous results...!"
    for item in ${WORKERS[*]}; do
        ssh $item.cs.umanitoba.ca 'rm -rf /tmp/hadoop'
    done
    rm -rf /tmp/hadoop

    echo "Clean done!"
}

function install {
    echo "Install on different machines..."

    {HADOOP_PREFIX}/bin/hadoop namenode -format

    for item in ${HELIUM[*]}; do
        scp /usr/local/hadoop/${HADOOP_VERSION}.tar.gz $item.cs.umanitoba.ca:/tmp/hao &&
        ssh $item.cs.umanitoba.ca "tar xf /tmp/hao/${HADOOP_VERSION}.tar.gz -C /tmp/hao/"
    done

    echo "Install Done!"
}

function start {
    echo "Starting..."

    ${HADOOP_PREFIX}/sbin/start-dfs.sh &&
        ${HADOOP_PREFIX}/sbin/start-yarn.sh
        #/tmp/hao/${SPARK_VERSION}/sbin/start-master.sh &&
        #/tmp/hao/${SPARK_VERSION}/sbin/start-slaves.sh

    echo "Start Done!"
}

function stop {
    echo "Stopping..."

    ${HADOOP_PREFIX}/sbin/stop-dfs.sh
    ${HADOOP_PREFIX}/sbin/stop-yarn.sh
    #/tmp/hao/${SPARK_VERSION}/sbin/stop-slaves.sh
    #/tmp/hao/${SPARK_VERSION}/sbin/stop-master.sh

    echo "Stop Done!"
}

if [ $1 == "clean" ]; then
    clean
elif [ $1 == "install" ]; then
    check && install
elif [ $1 == "start" ]; then
    start
elif [ $1 == "stop" ]; then
    stop
fi
