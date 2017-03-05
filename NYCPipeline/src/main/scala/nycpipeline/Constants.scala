package nycpipeline

import org.apache.spark.sql.types._

object Constants {

  val STATIC_DATA_URL = """https://gbfs.citibikenyc.com/gbfs/en/station_information.json"""
  val DYNAMIC_DATA_URL = """https://gbfs.citibikenyc.com/gbfs/en/station_status.json"""

  val DYNAMIC_SCHEMA = new (StructType)
    .add("last_updated", LongType)
    .add("ttl", IntegerType)
    .add("data",
      (new StructType)
        .add("stations",
          (new ArrayType(
            (new StructType)
              .add("station_id", StringType, true)
              .add("num_bikes_available", IntegerType, true)
              .add("num_bikes_disabled", IntegerType, true)
              .add("num_docks_available", IntegerType, true)
              .add("num_docks_disabled", IntegerType, true)
              .add("is_installed", IntegerType, true)
              .add("is_renting", IntegerType, true)
              .add("is_returning", IntegerType, true)
              .add("last_reported", StringType, true)
              .add("eightd_has_available_keys", BooleanType, true), true))))

  val STATIC_SCHEMA = new (StructType)
    .add("last_updated", LongType)
    .add("ttl", IntegerType)
    .add("data",
      (new StructType)
        .add("stations",
          (new ArrayType(
            (new StructType)
              .add("station_id", StringType, true)
              .add("name", StringType, true)
              .add("short_name", StringType, true)
              .add("lat", DoubleType, true)
              .add("lon", DoubleType, true)
              .add("region_id", IntegerType, true)
              .add("capacity", IntegerType, true)
              .add("eightd_has_key_dispenser", BooleanType, true)
              .add("rental_methods", DataTypes.createArrayType(StringType), true), true))))

}