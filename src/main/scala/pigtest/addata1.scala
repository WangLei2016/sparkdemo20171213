package pigtest

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object addata1 extends App {
  System.setProperty("hadoop.home.dir", "E:\\20171103\\hadoop-2.6.5")

      var sc: SparkConf=  new SparkConf().setAppName("addata1").setMaster("local[*]").set("spark.sql.warehouse.dir", "E:/sqarksql")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      var scc: SparkContext= new SparkContext(sc)
        var sql=  new SQLContext(scc)
       var file= scc.textFile("E:\\20171103\\ad\\ad_data1.txt")
     var data1 =  file.map(_.split("\\")).map(x=>{
               var keyword=x(0).trim()
                var campaign_id=x(1)
                 var date=x(2)
                  var time=x(3)
                   var display_site=x(4)
                    var was_clicked=x(5).toInt
                     var cpc=x(6).toInt
                      var country=x(7)
                      var placement=x(8)
                   
        addata1(keyword,campaign_id,date,time,display_site,was_clicked,cpc,country,placement)
         
       })
  
       var scd =sql.createDataFrame(data1)
     
       scd.createOrReplaceTempView("data")
      var  sb=new StringBuilder()
            // sb.append("select campaign_id,date,time,UPPER(keyword) keyword ,display_site,placement,was_clicked,cpc from data")
              sb.append("select count(*) from data")
       var  result=sql.sql(sb.toString()).show()
     // result.write.parquet("F:/data1")
     
   
       
}
case  class addata1(keyword:String,campaign_id:String,date:String,time:String,display_site:String
    ,was_clicked:Int,cpc:Int,country:String,placement:String)