hadoop-webarc-recread
=====================

Hadoop web archive (W/ARC) record reader

This experimental project is mainly a custom Hadoop RecordReader for native ARC/WARC files. It is based on the Hadoop 0.20 API (avoids deprecated “mapred” packages) and its purpose is to run experimental test with ARC containers on a Hadoop cluster. Test classes are included.

The project additionally includes a sample map / reduce program for web archive file identification using the Apache Tika™ 1.0 API. Input is a HDFS directory path containing the test ARC files. Output of the map / reduce  program is a mime time distribution list of the analyzed input (all identified MIME types inside each ARC plus the occurrence of each type in all ARCs).


Usage of the sample program:
        
hadoop jar ./tb-wc-hd-archd.jar -D mapred.reduce.tasks=1 [DirToBeScanned] [DirOutput]
