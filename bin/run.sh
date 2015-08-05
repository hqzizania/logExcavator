/home/qhuang/works-2.0/sync/spark/spark-1.4.0/bin/spark-submit \
    --class com.intel.logExcavator.Excavator \
    --master spark://sr464:7077 target/scala-2.10/logExcavator-assembly-0.0.1.jar \
    --master spark://sr464:7077 \
    --slices 216 \
    --k 50 \
    --algorithm word2Vec \
    --hdfs hdfs://sr464:9000/ \
    --jobname 7days \
    --checkpointDir hdfs://sr464:9000/checkpoint \
    hdfs://sr464:9000/messages.7days
