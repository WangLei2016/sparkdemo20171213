package day03
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel

object WindowWordCount {
  def main(args:Array[String]) ={
    System.setProperty("hadoop.home.dir", "E:\\20171103\\hadoop-2.6.5");

    val conf=new SparkConf().setAppName("WindowWordCount").
setMaster("local[*]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("F:/checkpoint")
    val lines=ssc.socketTextStream(	"localhost",
8889.toInt,
StorageLevel.MEMORY_ONLY_SER)
    val words= lines.flatMap(_.split(","))
/*采用reduceByKeyAndWindow操作进行叠加处理，窗口时间间隔与滑动时间间隔分别由参数args(2)和args(3)给出。*/
    val wordcounts=words.map(x=>(x,1)).
    reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),
        Seconds(30.toInt),Seconds(10.toInt))
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}