#!/usr/bin/bash

SPARK_VERSION="spark-1.3.1-bin-hadoop2.6"
HADOOP_VERSION="hadoop-2.6.0"
HELIUM=(helium-01 helium-02 helium-03 helium-04)
HADOOP_DIR="/tmp/hao/${HADOOP_VERSION}"
SPARK_DIR="/tmp/hao/${SPARK_VERSION}"

function check {
    echo "Checking the files.."
    if [ -f /usr/local/spark/${SPARK_VERSION}.tgz ] ;then
        echo "find spark..."
    else
        echo "can't find spark"
        exit
    fi

    if [ -f /usr/local/hadoop/${HADOOP_VERSION}.tar.gz ] ;then
        echo "find hadoop..."
    else
        echo "can't find hadoop"
        exit
    fi
    echo "Check done!"
}

function clean {
    echo "Clean previous results...!"
    for item in ${HELIUM[*]}; do
        ssh $item.cs.umanitoba.ca 'rm -rf /tmp/*'
    done
    rm -rf /tmp/hao

    echo "Clean done!"
}

function install {
    echo "Install on different machines..."

    mkdir -p /tmp/hao/hdfs/name

    cp /usr/local/hadoop/${HADOOP_VERSION}.tar.gz /tmp/hao &&
    tar xf /tmp/hao/${HADOOP_VERSION}.tar.gz -C /tmp/hao/ &&
    cp -rf /usr/local/hadoop/etc/hadoop /tmp/hao/${HADOOP_VERSION}/etc/

    cp /usr/local/spark/${SPARK_VERSION}.tgz /tmp/hao &&
    tar xf /tmp/hao/${SPARK_VERSION}.tgz -C /tmp/hao/ &&
    cp -rf /usr/local/spark/conf/ /tmp/hao/${SPARK_VERSION}/

    /tmp/hao/${HADOOP_VERSION}/bin/hadoop namenode -format

    for item in ${HELIUM[*]}; do
        ssh $item.cs.umanitoba.ca 'mkdir -p /tmp/hao/hdfs/data'

        scp /usr/local/hadoop/${HADOOP_VERSION}.tar.gz $item.cs.umanitoba.ca:/tmp/hao &&
        ssh $item.cs.umanitoba.ca "tar xf /tmp/hao/${HADOOP_VERSION}.tar.gz -C /tmp/hao/" &&
        scp /usr/local/hadoop/etc/worker/* $item.cs.umanitoba.ca:/tmp/hao/${HADOOP_VERSION}/etc/hadoop/

        scp /usr/local/spark/${SPARK_VERSION}.tgz $item.cs.umanitoba.ca:/tmp/hao &&
        ssh $item.cs.umanitoba.ca "tar xf /tmp/hao/${SPARK_VERSION}.tgz -C /tmp/hao/" &&
        scp /usr/local/spark/conf/worker/* $item.cs.umanitoba.ca:/tmp/hao/${SPARK_VERSION}/conf/
    done

    echo "Install Done!"
}

function start {
    echo "Starting..."

    /tmp/hao/${HADOOP_VERSION}/sbin/start-dfs.sh &&
        sudo /tmp/hao/${SPARK_VERSION}/sbin/start-master.sh &&
        /tmp/hao/${SPARK_VERSION}/sbin/start-slaves.sh

    echo "Start Done!"
}

function stop {
    echo "Stopping..."

    /tmp/hao/${HADOOP_VERSION}/sbin/stop-dfs.sh
    /tmp/hao/${SPARK_VERSION}/sbin/stop-slaves.sh
    sudo /tmp/hao/${SPARK_VERSION}/sbin/stop-master.sh

    echo "Stop Done!"
}

if [ $1 == "clean" ]; then
    stop && clean
elif [ $1 == "install" ]; then
    check && install
elif [ $1 == "start" ]; then
    start
elif [ $1 == "stop" ]; then
    stop
fi
