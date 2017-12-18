package day03
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.StreamingContext._
object StreamingFileWordCount {
  def main(args: Array[String]): Unit ={
//以local模式运行，并设定master节点工作线程数为2。
    val sparkConf =	new SparkConf().
setAppName("StreamingFileWordCount").
setMaster("local[2]")
/*创建StreamingContext实例，设定批处理时间间隔为20秒。*/ 
    val ssc = new StreamingContext(sparkConf,Seconds(20))
/*指定数据源来自本地home/dong/Streamingtext。*/
    val lines = ssc.textFileStream("/home/dong/Streamingtext")
/*在每个批处理时间间隔内，对指定文件夹中变化的数据进行单词统计并打印。*/
    val words= lines.flatMap(_.split(" "))
    val wordcounts=words.map(x=>(x,1)).reduceByKey(_+_)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}