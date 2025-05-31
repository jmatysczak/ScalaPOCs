package jmat.sparkstructuredstreaming

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MicroBatchExecution
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

object Main {
  def main(args: Array[String]) : Unit = {

    println("Current working directory: " + Paths.get(".").toAbsolutePath)
    println("Current process name: " + ManagementFactory.getRuntimeMXBean().getName())
    println("SPARK_HOME: " + System.getenv("SPARK_HOME"))

    // When using the master "local-cluster" the following environment variables must be set:
    // - SPARK_SCALA_VERSION=2.13
    // - SPARK_HOME=Directory to spark installation
    // local-cluster[2, 2, 2048] means 2 works and each worker thinks it has 2 cores and 2g of RAM to allocate for executors. So 4 cores and 4gb of RAM total.
    val spark = SparkSession.builder()
      .appName("POC: Spark Session")
      .master("local")
      //.master("local-cluster[2, 2, 2048]")
      .config("spark.jars", "build/libs/app.jar")
      .config("spark.executor.cores", 1)
      .config("spark.executor.memory", "512m")
      .getOrCreate()

    import spark.implicits._

    spark.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = {
        println("onQueryStarted: id: " + event.id)
        println("onQueryStarted: run id: " + event.runId)
        println("onQueryStarted: timestamp: " + event.timestamp)
      }

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        println("")
        println("---------------")
        println("onQueryProgress")
        println("---------------")
        // With checkpointing on, this id stays the same across restarts.
        println("onQueryProgress: id: " + event.progress.id)
        // This id changes across restarts.

        println("onQueryProgress: run id: " + event.progress.runId)
        println("onQueryProgress: timestamp: " + event.progress.timestamp)

        // With checkpointing on, this id is unique across restarts.
        // That is, if the last processed batch id is 30 and the process
        // is restarted the first batch id after restart will be 31.
        println("onQueryProgress: batch id: " + event.progress.batchId)
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        println("onQueryTerminated: id: " + event.id)
        println("onQueryTerminated: run id: " + event.runId)
      }
    })

    val rateData = spark.readStream
      .format("rate")
      .option("numPartitions", 8)
      .option("rowsPerSecond", 24)
      .load()

    println("Schema:")
    rateData.printSchema()

    val partitionFailureFile = "/tmp/SparkStructuredStreamingPartitionFailureFile.txt"
    Files.deleteIfExists(Paths.get(partitionFailureFile))

    val rateDataTransformed = rateData.mapPartitions { input =>
      println("")
      println("-------------")
      println("mapPartitions")
      println("-------------")

      // Process name is a platform independent way of getting the process id (apparently).
      // It is in the form: processId@hostname
      val processName = ManagementFactory.getRuntimeMXBean().getName()
      val threadId = Thread.currentThread().getId
      // This gets the Spark Structured Streaming Batch ID which is unique and sequential across restarts with checkpointing.
      // The Batch ID will be the same for all partitions in the same batch/stage.
      val batchId = TaskContext.get().getLocalProperty(MicroBatchExecution.BATCH_ID_KEY)

      println("Current process name: " + processName)
      println("Thread ID: " + threadId)
      println("Batch ID: " + batchId)

      // A stage here is similar to a stage in batch.
      // It is a collection/batch of tasks working off partitions.
      // I think this is the unit of work that is committed.
      // Appears to start at 1 on first run. Starts at 0 after restart with checkpoint.
      // Although, this could be because of the forced test failure.
      // Batch ID and Stage ID are similar in that they are the same for all partitions in the same Batch/Stage.
      // However, when checkpointing is on Batch ID will not reset between restarts. Where as Stage ID does.
      // Appears that it will be the same as the Batch ID on first run or if there is no checkpoint.
      println("TaskContext Stage ID: " + TaskContext.get().stageId())

      // Not sure what this is. I am guessing it indicates a retry of a stage.
      // Starts at 0.
      println("TaskContext Stage Attempt Number: " + TaskContext.get().stageAttemptNumber())

      // Sequential and unique number for a task across stages.
      // That is, this number uniquely identifies a task regardless of stage.
      // That is, it uniquely identifies a partition as long as there isn't a restart.
      // It is not tied to partition. So if a partition fails, the partition is retried
      // in a task with another Task Attempt ID.
      // Appears to start at 1 on first run. Starts at 0 after restart with checkpoint.
      // Although, this could be because of the forced test failure.
      println("TaskContext Task Attempt ID: " + TaskContext.get().taskAttemptId())

      // Number that indicates the attempt of a partition.
      // That is, if a partition fails, this number is incremented on the next attempt.
      // Starts at 0.
      println("TaskContext Partition Attempt Number: " + TaskContext.get().attemptNumber())

      // The next two numbers are the same. They are the id of the partition within a stage.
      // That is, this number is not unique across stages.
      // It is only unique for the same Batch ID.
      // It is only unique for the same Stage ID.
      // A partition is uniquely identified by the stage id and this number.
      // Starts at 0.
      println("TaskContext Partition ID: " + TaskContext.get().partitionId())
      println("TaskContext Partition ID: " + TaskContext.getPartitionId())

      if(true || Files.exists(Paths.get(partitionFailureFile))) {
        Files.deleteIfExists(Paths.get(partitionFailureFile))

        val values = input
          .map(row => row.getAs[Long]("value"))
          .toArray

        Iterator.single(ExecutorInfo(processName, threadId, batchId, values))
      } else {
        Files.write(Paths.get(partitionFailureFile), Array.emptyByteArray)
        throw new Exception("Forcing partition failure.")
      }
    }

    val query = rateDataTransformed.writeStream
      .queryName("POC: Spark Query")
      .outputMode(OutputMode.Append)
      .format("console")
      //.option("checkpointLocation", "/tmp/StreamingPOCCheckpointLocation/")
      .option("truncate", false)
      .start()

    print("\nWaiting for query to terminate...\n")
    query.awaitTermination(30000)
    print("\nAwait terminate timed out...stopping...\n")
    query.stop()

    println("Done!")
  }
}

case class ExecutorInfo(processName: String, threadId: Long, batchId: String, values: Seq[Long])
