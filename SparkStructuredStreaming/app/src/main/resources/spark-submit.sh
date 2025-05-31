$SPARK_HOME/bin/spark-submit \
  --master spark://localhost:7077 \
  --class jmat.sparkstructuredstreaming.Main \
  --executor-cores 1 \
  --executor-memory 512m \
  ../../../build/libs/app.jar
