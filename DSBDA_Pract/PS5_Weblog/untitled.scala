/*Problem Statement No. 05
Write a Scala Program to process a log file of a system and perform following analytics on the given dataset.
(I) Display the list of top 10 frequent hosts.
(II) Display the list of top 5 URLs or paths
(III) Display the number of unique Hosts
(I) Display the count of 404 Response Codes
(II) Display the list of Top Twenty-five 404 Response Code Hosts
(III) Display the number of Unique Daily Hosts
*/

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract, sum, col,to_date, udf, to_timestamp, desc,dayofyear,year}

val spark = SparkSession.builder().appName("Weblog").master("local[*]").getOrCreate()
val base_df = spark.read.text("weblog.csv")
base_df.printSchema()
base_df.show(3,false)

val parsed_df = base_df.select(regexp_extract($"value","""^([^(\s|,)]+)""",1).alias("host"),
    regexp_extract($"value","""^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("timestamp"),
    regexp_extract($"value","""^.*\w+\s+([^\s]+)\s+HTTP.*""",1).as("path"),
    regexp_extract($"value","""^.*,([^\s]+)$""",1).cast("int").alias("status"))
parsed_df.printSchema()
parsed_df.show(5,false)

println("bad data: " + base_df.filter($"value".isNull).count())

val bad_rows_df = parsed_df.filter($"host".isNull || $"timestamp".isNull || $"path".isNull || $"status".isNull)
println("number of bad rows: " + bad_rows_df.count())

val cleaned_df = parsed_df.na.drop()

val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar" ->3,"Apr" -> 4, "May" -> 5, "Jun" -> 6, "Jul" -> 7, "Aug" -> 8
    , "Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)

def parse_clf_time(s: String) = {
    "%3$s-%2$s-%1$s %4$s:%5$s:%6$".format(s.substring(0,2),month_map(s.substring(3,6)),s.substring(7,11)
    ,s.substring(12,14),s.substring(15,17),s.substring(18))
}

val toTimestamp = udf[String,String](parse_clf_time(_))
val logs_df = cleaned_df.select($"*",to_timestamp(toTimestamp($"timestamp")).alias("time)).drop("timestamp")
logs_df.printSchema()
logs_df.show(2)

logs_df.groupby("path").count().sort(desc("count")).ahow(10)

logs_df.groupby("host").count().filter($"count" > 10).show()

val unique_host_count = logs_df.select("host").distinct().count()
println("Unique hosts :" + (unique_host_count))


val not_found = logs_df.where($"status" === 404).cache()
println("found %d 404 Urls".format(not_found_df.count()))


not_found_df.groupBy("path").count().sort("count").show(20,false)