package nycpipeline

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.TimestampType

object ProcessLayer {

  def processStaticData(rawDF: DataFrame, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    val staticDataDF = rawDF.select(explode($"data.stations").as("stations_flat"))

    val staticStationsDF = staticDataDF.select("stations_flat.station_id",
      "stations_flat.name",
      "stations_flat.short_name",
      "stations_flat.lat",
      "stations_flat.lon",
      "stations_flat.region_id",
      "stations_flat.rental_methods",
      "stations_flat.capacity",
      "stations_flat.eightd_has_key_dispenser")

    staticStationsDF

  }

  def processDynamicData(rawDF: DataFrame, sqlContext: SQLContext): DataFrame = {

    import sqlContext.implicits._

    val dynamicDF = rawDF.select(explode($"data.stations").as("stations_flat"))
    val dynamicStationsDF = dynamicDF.select($"stations_flat.station_id",
      $"stations_flat.num_bikes_available",
      $"stations_flat.num_bikes_disabled",
      $"stations_flat.num_docks_available",
      $"stations_flat.num_docks_disabled",
      $"stations_flat.is_installed",
      $"stations_flat.is_renting",
      $"stations_flat.is_returning",
      $"stations_flat.last_reported".cast(LongType).cast(TimestampType),
      $"stations_flat.eightd_has_available_keys")

    dynamicStationsDF

  }

}