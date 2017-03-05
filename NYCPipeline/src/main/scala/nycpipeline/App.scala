package nycpipeline

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object App {

  def main(args: Array[String]): Unit = {

    val cassandraIP = args(0)
    val keyspace = args(1)
    val staticTableName = args(2)
    val dynamicTableName = args(3)

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", args(0))
    val sc = new SparkContext("local[*]", "NYC Bike Rentals", conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Static Data Ingestion/Processing/Storage
    val ingestedStaticData = IngestionLayer.ingestStaticData(sc, sqlContext)
    val processedStaticData = ProcessLayer.processStaticData(ingestedStaticData, sqlContext)
    StorageLayer.dumpStaticData(processedStaticData, sqlContext, keyspace, staticTableName)

    // Streaming Data Ingestion/Processing/Storage
    val ssc = new StreamingContext(sc, Seconds(5))
    val data = ssc.receiverStream(new DynamicReceiver(Constants.DYNAMIC_DATA_URL))

    data.foreachRDD { element =>

      val dynamicDataRawDF = sqlContext.read.schema(Constants.DYNAMIC_SCHEMA).json(element)
      val processedDynamicData = ProcessLayer.processDynamicData(dynamicDataRawDF, sqlContext)
      StorageLayer.dumpDynamicData(processedDynamicData, sqlContext, keyspace, dynamicTableName)

    }
    ssc.start()
    ssc.awaitTermination()

  }

}