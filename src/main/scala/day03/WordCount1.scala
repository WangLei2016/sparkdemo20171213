package com.wl.test.word


  import org.apache.spark.SparkContext
 import org.apache.spark.SparkContext._
 import org.apache.spark.SparkConf
 
object WordCount {
    def main(args: Array[String]) {

     // System.setProperty("hadoop.home.dir", "H:\\hadoop_windos\\hadoopHome\\hadoop-2.6.5");


      val inputFile =  "hdfs://localhost:8020/tmp/shakespeare.txt"
     // val inputFile =  "C:/Users/asus/Desktop/a.txt"
        //val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val conf = new SparkConf().setAppName("WordCount")
        val sc = new SparkContext(conf)
        // 数据格式为访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
     
           //  计算用户查询次数
                val textFile = sc.textFile(inputFile)
                //.计算用户查询次数排行榜（降序）
               val mappilt= textFile.flatMap(_.split("")).map(x=>(x,1)).reduceByKey(_+_).sortBy(x => x._2, false, 1)
               //取前三
            //val num: Array[(String, Int)]= mappilt.take(3)
               
            //list转化为RDD
          //sc.parallelize(num).saveAsTextFile("E:\\test")


          println(mappilt.collect().foreach(print))

          
          
}
}