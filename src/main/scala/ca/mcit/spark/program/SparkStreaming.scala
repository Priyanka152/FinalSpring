package ca.mcit.spark.program

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App with Base {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Spark streaming ")
    .master("local[*]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val kafkaconfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "quickstart.cloudera:9092",
    ConsumerConfig.GROUP_ID_CONFIG -> "trip",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
  )
  val enrichedStation = spark.read.csv("/user/fall2019/priyanka/spring2/output.csv")
        .toDF("station_id", "short_name", "name", "has_kiosk", "capacity",
  "eightd_has_key_dispenser", "external_id", "rental_method", "electric_bike_surcharge_waiver",
  "lat", "lon","is_charging", "system_id", "email", "start_Date", "language",
  "phone_number", "timezone", "url")

  enrichedStation.createOrReplaceTempView("stationInformation")
  val stationDf:DataFrame=spark.sql(
    "SELECT stationInformation.system_id ," +
      "stationInformation.timezone," +
      "stationInformation.short_name," +
      "stationInformation.name," +
      "stationInformation.lat," +
      "stationInformation.lon," +
      "stationInformation.capacity " +
      "FROM stationInformation"
  )
  val topic = "fall2019_priyanka_trip"
  val messages: DStream[ConsumerRecord[String, String]] =
    KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](List(topic), kafkaconfig)
    )
  import spark.implicits._
  val lines: DStream[String] = messages.map(_.value())
  lines.foreachRDD(rdd=> {
    val trip: RDD[Trip] = rdd.map(Trip(_))
    val tripDf = trip.toDF()

    tripDf
          .join(stationDf,tripDf("start_station_code") ===  stationDf("short_name"),"left")
          .write.mode(SaveMode.Append)
          .option("header", "false")
          .option("inferSchema", "true")
          .csv("/user/fall2019/priyanka/spring3/enriched_trip.csv")
  })
  ssc.start()
  ssc.awaitTermination()
  ssc.stop(true)
}
