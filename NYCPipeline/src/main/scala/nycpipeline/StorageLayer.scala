package nycpipeline

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra
import scala.util.Try

object StorageLayer {

  def dumpStaticData(processedDF: DataFrame, sqlContext: SQLContext, keyspace:String, staticTable:String) {

    processedDF.createCassandraTable(keyspace, staticTable)

    processedDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> staticTable, "keyspace" -> keyspace))
      .mode(SaveMode.Append)
      .save()

  }

  def dumpDynamicData(processedDF: DataFrame, sqlContext: SQLContext, keyspace:String, dynamicTable:String) {

    if (Try(processedDF.createCassandraTable(keyspace, dynamicTable)).isSuccess){
      processedDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> dynamicTable, "keyspace" -> keyspace))
      .mode(SaveMode.Append)
      .save()
  
    }else{
      processedDF.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> dynamicTable, "keyspace" -> keyspace))
      .mode(SaveMode.Append)
      .save()

    }

    
  }

}