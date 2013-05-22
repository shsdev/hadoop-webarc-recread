hadoop-webarc-recread
=====================

Introduction
------------

Hadoop web archive (W/ARC) record reader

This project contains custom Hadoop RecordReader classes for native ARC/WARC 
files based on the Hadoop 0.20 API.

The project includes a sample MapReduce application which reads ARC files from
a given HDFS path and returns a mime type distribution. 

Install
-------

    cd hadoop-webarc-recread
    mvn install 

Usage
-------

    hadoop jar
      target/hadoop-webarc-recread-1.0-SNAPSHOT-jar-with-dependencies.jar -n
      job_name -d /path/to/hdfs/input/directory -n hadoop_job_name
    -d,--dir <arg>    HDFS directory web archive container files.
    -n,--name <arg>   Job name.

