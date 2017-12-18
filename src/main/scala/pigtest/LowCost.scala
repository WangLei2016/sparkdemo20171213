package pigtest

import org.apache.spark.sql.SparkSession

object LowCost extends App {
    System.setProperty("hadoop.home.dir", "E:\\20171103\\hadoop-2.6.5")
    
    
          var ss: SparkSession= SparkSession.builder().appName("addata2").master("local[*]")
       .config("spark.sql.warehouse.dir", "E:/sqarksql")
       .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
       .getOrCreate()

        var file=ss.read.parquet("F:\\data1")
  
        file.createOrReplaceTempView("data1")
        
         var  sb=new StringBuilder()
     // sb.append("select sum(cpc) cpc, display_site   from data1  group by  display_site order by cpc ")
        sb.append("select count(*) from data1 ")
       //Thread.sleep(50000)  
         
       ss.sql(sb.toString()).show(10)
      
  
  
}