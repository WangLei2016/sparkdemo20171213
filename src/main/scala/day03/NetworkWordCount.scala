package day03
import org.apache.spark.streaming.{Milliseconds,Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object NetworkWordCount {
    def main (args:Array[String]) ={
//以local模式运行，并设定master节点工作线程数为2。
    val conf=new SparkConf().setAppName("NetworkWordCount").
               setMaster("local[2]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc, Seconds(5))
/*通过socketTextStream获取指定节点指定端口的数据创建DStream，并保存在内存和硬盘中，其中节点与端口分别对应参数args(0)和args(1)。*/

val lines=ssc.socketTextStream("localhost",
8889.toInt,
StorageLevel.MEMORY_AND_DISK_SER)
//在每个批处理时间间隔内对获取到的数据进行单词统计并且打印。
    val words= lines.flatMap(_.split(","))
    val wordcounts = words.map(x=>(x,1)).reduceByKey(_+_)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}