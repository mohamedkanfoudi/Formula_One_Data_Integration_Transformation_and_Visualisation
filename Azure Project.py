# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Secret02",key="ClientSecret")

spark.conf.set("fs.azure.account.auth.type.azureprojectmk2023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azureprojectmk2023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azureprojectmk2023.dfs.core.windows.net", "85c030e5-d1af-4660-961f-e52428c3133f")
spark.conf.set("fs.azure.account.oauth2.client.secret.azureprojectmk2023.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureprojectmk2023.dfs.core.windows.net", "https://login.microsoftonline.com/d001e3df-c491-489f-9fe5-7e6bdf26c1aa/oauth2/token")

# COMMAND ----------



circuits_df = (spark.read.json("abfss://raw@azureprojectmk2023.dfs.core.windows.net/input/results.json"))

#display(circuits_df)
#circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,explode,from_json
from pyspark.sql.types import StringType,StructType,StructField

# COMMAND ----------



exploded_df1 = circuits_df.select(explode(col("MRData.RaceTable.Races")).alias("circuit1") , col("MRData.limit"), col("MRData.offset"), col("MRData.series") , col("MRData.total") , col("MRData.url") , col("MRData.xmlns") )
#exploded_df1 = exploded_df2.select(explode(col("circuit2.Laps")).alias("circuit1")  , col("circuit2.Circuit.circuitId").alias("circuit_id"),  col("circuit2.Circuit.circuitName").alias("circuit_name"), col("circuit2.Circuit.Location.country"),  col("circuit2.Circuit.Location.lat").alias("latitude") , col("circuit2.Circuit.Location.locality") ,  col("circuit2.Circuit.Location.long").alias("longitude"), col("circuit2.date") , col("circuit2.raceName")  ,col("circuit2.round")  ,col("circuit2.season")  ,col("circuit2.time")  , col("limit") , col("offset") , col("series"), col("total"), col("url"), col("xmlns"))

exploded_df = exploded_df1.select(explode(col("circuit1.Results")).alias("circuit") , col("circuit1.Circuit.circuitId").alias("circuit_id"),  col("circuit1.Circuit.circuitName").alias("circuit_name"), col("circuit1.Circuit.Location.country"),  col("circuit1.Circuit.Location.lat").alias("latitude") , col("circuit1.Circuit.Location.locality") ,  col("circuit1.Circuit.Location.long").alias("longitude") ,col("circuit1.date"),col("circuit1.raceName"),col("circuit1.round"),col("circuit1.season"),col("circuit1.time") ,col("limit") , col("offset") , col("series"), col("total"), col("url"), col("xmlns"))

# COMMAND ----------


circuits_selected_df = exploded_df.drop("url")



# COMMAND ----------


circuits_renamed_df = circuits_selected_df.withColumn("donstructor_id" , col("circuit.Constructor.constructorId")).withColumn("constructor_name" , col("circuit.Constructor.name")).withColumn("constructor_nationality" , col("circuit.Constructor.nationality")).withColumn("driver_code" , col("circuit.Driver.code")).withColumn("driver_dateOfBirth" , col("circuit.Driver.dateOfBirth")).withColumn("driver_id" , col("circuit.Driver.driverId")).withColumn("driver_familyName" , col("circuit.Driver.familyName")).withColumn("driver_givenName" , col("circuit.Driver.givenName")).withColumn("driver_nationality" , col("circuit.Driver.nationality")).withColumn("driver_permanentNumber" , col("circuit.Driver.permanentNumber")).withColumn("speed" , col("circuit.FastestLap.AverageSpeed.speed")).withColumn("units" , col("circuit.FastestLap.AverageSpeed.units")).withColumn("fastestLap_time" , col("circuit.FastestLap.Time.time")).withColumn("lap" , col("circuit.FastestLap.lap")).withColumn("rank" , col("circuit.FastestLap.rank")).withColumn("circuit_millis" , col("circuit.Time.millis")).withColumn("circuit_time" , col("circuit.Time.time")).withColumn("grid" , col("circuit.grid")).withColumn("laps" , col("circuit.laps")).withColumn("number" , col("circuit.number")).withColumn("points" , col("circuit.points")).withColumn("position" , col("circuit.position")).withColumn("positionText" , col("circuit.positionText")).withColumn("status" , col("circuit.status")).drop("circuit")


circuits_renamed_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date" , current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@azureprojectmk2023.dfs.core.windows.net/input/results")

# COMMAND ----------

display(spark.read.parquet("abfss://processed@azureprojectmk2023.dfs.core.windows.net/input/results"))

# COMMAND ----------

raw_folder_path  = "abfss://raw@azureprojectmk2023.dfs.core.windows.net/input/"
processed_folder_path= "abfss://processed@azureprojectmk2023.dfs.core.windows.net/input/"
presentation_folder_path = "abfss://presentation@azureprojectmk2023.dfs.core.windows.net/input/"

# COMMAND ----------

drivers_df = spark.read.parquet(processed_folder_path+"drivers") \
.withColumnRenamed("number" , "driver_number") \
.withColumnRenamed("driver_givenName" , "driver_name") \
    .withColumnRenamed("driver_givenName" , "driver_name") 

# COMMAND ----------

constructors_df = spark.read.parquet(processed_folder_path+"constructors") \
.withColumnRenamed("constructor_name" , "team_name") 

# COMMAND ----------

circuits_df = spark.read.parquet(processed_folder_path+"circuits") 

# COMMAND ----------

races_df = spark.read.parquet(processed_folder_path+"races") \
.withColumnRenamed("raceName" , "race_name") \
.withColumnRenamed("circuit_date" , "race_date") 

# COMMAND ----------

results_df = spark.read.parquet(processed_folder_path+"results") \
.withColumnRenamed("time" , "race_time") \
.withColumnRenamed("driver_nationality" , "driver_nationality1") \
.withColumnRenamed("grid" , "grid1") \
.withColumnRenamed("points" , "points1") \
    .withColumnRenamed("position" , "position1") \
         .withColumnRenamed("locality" , "location") \
             .withColumnRenamed("lap" , "fastest_lap") \
.withColumnRenamed("donstructor_id","constructor_id")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import when
condition = col("locality") == "Casablanca"  # Example condition: id is 2
new_value = "spa"              # New value to set
circuits_df = circuits_df.withColumn("circuit_id", when(condition, new_value).otherwise(col("circuit_id")))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

display(
     results_df.join(races_df, results_df.circuit_id== races_df.circuit_id)\
    .join(circuits_df, races_df.circuit_id== circuits_df.circuit_id)
        )

# COMMAND ----------

race_results_df =  results_df.join(races_df, results_df.circuit_id== races_df.circuit_id)\
    .join(circuits_df, races_df.circuit_id== circuits_df.circuit_id)

# COMMAND ----------

final_df = race_results_df.select( "race_name", "race_date", "location", "driver_familyName", "number", "driver_nationality1","constructor_name", "grid1", "fastest_lap", "race_time", "points1", "position1")

# COMMAND ----------

final_df.write.mode("overwrite").parquet(presentation_folder_path+"race_results")

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_name == 'Belgian Grand Prix'").orderBy(final_df.points.desc()))


# COMMAND ----------

race_results_df = spark.read.parquet(presentation_folder_path+"race_results") 

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum,when,count,col

# COMMAND ----------

drivers_standings_df = race_results_df.groupBy("race_date" , "driver_familyName" ,"driver_nationality1" , "constructor_name").agg(sum("points1").alias("total_points"),count(when(col("position1") == 1 , True)).alias("wins"))

# COMMAND ----------

display(drivers_standings_df)

# COMMAND ----------

drivers_standings_df.write.mode("overwrite").parquet(presentation_folder_path+"driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists formula1_db;

# COMMAND ----------

raceresults_df = spark.read.parquet(presentation_folder_path+"race_results")
raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_db.race_results")


# COMMAND ----------

driverStandings_df = spark.read.parquet(presentation_folder_path+"driver_standings")
#raceresults_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_db.driver_standings")


# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_familyName, race_date ,position1,points1 from formula1_db.race_results where position1=1 and year(race_date) between 2013 and 2023;

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_familyName , sum(points1) as total_points , count(1) as total_races from formula1_db.race_results
# MAGIC group by driver_familyName
# MAGIC order by total_points desc

# COMMAND ----------


