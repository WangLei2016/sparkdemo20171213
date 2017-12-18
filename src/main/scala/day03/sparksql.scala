package day03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession




object  sparksql  {
       def main(args: Array[String]) {

            System.setProperty("hadoop.home.dir", "E:\\20171103\\hadoop-2.6.5");

  //val inputFile =  "E:/SogouQ.sample"
     val inputFile =  "C:/Users/asus/Desktop/a.txt"
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]").set("spark.sql.warehouse.dir", "E:/sqarksql")  

   var sqlsession= SparkSession.builder().config(conf) .getOrCreate()
//数据导入方式  
    val readjson=  sqlsession.read.json(inputFile)
     
     //查看表  
     val a: Unit=readjson.show()
     
      //查看表结构  
  val b: Unit=  readjson.printSchema()
  
readjson.createOrReplaceTempView("persion")
//readjson.createTempView(viewName)
var c=sqlsession.sql("select * from persion  limit 1  ")
        

        c.show()
        
        sqlsession.stop() 
        
        }
  
  
}