/home/qhuang/works-2.0/sync/spark/spark-1.4.0/bin/spark-submit \
    --class com.intel.logExcavator.Excavator \
    --master yarn-client \
    --num-executors 18 \
    --driver-memory 10g \
    --executor-memory 15g \
    --executor-cores 5 \
    target/scala-2.10/logExcavator-assembly-0.0.1.jar \
    --master yarn-client \
    --slices 96 \
    --k 50 \
    --algorithm word2Vec \
    --hdfs hdfs://sr464:9000/ \
    --jobname 1day \
    --checkpointDir hdfs://sr464:9000/checkpoint \
    hdfs://sr464:9000/messages
