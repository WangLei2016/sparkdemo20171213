package pigtest

import org.apache.spark.sql.SparkSession
import java.sql.Struct
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

object addata2 extends App {

  System.setProperty("hadoop.home.dir", "E:\\20171103\\hadoop-2.6.5");

  var ss: SparkSession = SparkSession.builder().appName("addata2").master("local[*]")
    .config("spark.sql.warehouse.dir", "E:/sqarksql")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

 // var srt = ss.read.textFile("E:\\20171103\\ad\\ad_data2.txt")
    var srt = ss.read.textFile("hdfs://192.168.2.10:9000/dualcore/ad_data2.txt")

  var file = srt.rdd.map(x => {
    var lines: Array[String] = x.split(",")
    var campaign_id = lines(0)
    var date = lines(1).replace("-", "/")
    var time = lines(2)
    var display_site = lines(3)
    var placement = lines(4)
    var was_clicked = lines(5).toInt
    var cpc = lines(6).toInt
    var keyword = lines(7)

    Row(campaign_id, date, time, display_site, placement, was_clicked, cpc, keyword)
  })


  
  var schem = StructType(
    Seq(
      StructField("campaign_id", StringType, true),
      StructField("date", StringType, true),
      StructField("time", StringType, true),
      StructField("display_site", StringType, true),
      StructField("placement", StringType, true),
      StructField("was_clicked", IntegerType, true),
      StructField("cpc", IntegerType, true),
      StructField("keyword", StringType, true)))
      
      
      
      var result=ss.createDataFrame(file, schem)
      result.createTempView("data2")
      ss.sql("select * from data2").show()

}