package nycpipeline

import org.apache.spark.sql.DataFrame
import java.net.URL
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object IngestionLayer {

  def ingestStaticData(sc: SparkContext, sqlContext: SQLContext): DataFrame = {

    val staticDataUrl = new URL(Constants.STATIC_DATA_URL)
    val staticDataResult = scala.io.Source.fromURL(staticDataUrl).mkString

    val staticDataRdd = sc.parallelize(staticDataResult :: Nil)
    val staticDataRawDF = sqlContext.read.schema(Constants.STATIC_SCHEMA).json(staticDataRdd)

    staticDataRawDF

  }

}