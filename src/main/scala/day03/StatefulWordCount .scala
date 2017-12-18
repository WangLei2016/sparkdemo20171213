package day03
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Milliseconds,Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
object StatefulWordCount  {
  def main(args:Array[String]): Unit ={
/*定义更新状态方法，参数values为当前批处理时间间隔内各单词出现的次数，state为以往所有批次各单词累计出现次数。*/
    val updateFunc=(values: Seq[Int],state:Option[Int])=>{
		val currentCount=values.foldLeft(0)(_+_)
		val previousCount=state.getOrElse(0)
		Some(currentCount+previousCount)
    }
    val conf=new SparkConf().
setAppName("StatefulWordCount").
setMaster("local[*]")
    val sc=new SparkContext(conf)
//创建StreamingContext，Spark Steaming运行时间间隔为5秒。
    val ssc=new StreamingContext(sc, Seconds(5))
/*使用updateStateByKey时需要checkpoint持久化接收到的数据。在集群模式下运行时，需要将持久化目录设为HDFS上的目录。*/
	ssc.checkpoint("F:/checkpoint")
/*通过Socket获取指定节点指定端口的数据创建DStream，其中节点与端口分别由参数args(0)和args(1)给出。*/
    val lines=ssc.socketTextStream("localhost",8889.toInt)
    val words=lines.flatMap(_.split(","))
    val wordcounts=words.map(x=>(x,1))
//使用updateStateByKey来更新状态，统计从运行开始以来单词总的次数。
    val stateDstream=wordcounts.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}