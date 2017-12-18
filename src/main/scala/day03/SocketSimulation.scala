package day03

import java.io.{PrintWriter}
import java.net.ServerSocket
import scala.io.Source

object SocketSimulation {
  //定义随机获取整数的方法。
  def index(length: Int)={
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }
  def main(args:Array[String]): Unit ={
//    if(args.length!=3){
///*调用数据流模拟器需要三个参数：文件路径、端口号和批处理时间间隔时间（单位：毫秒）。*/
//      System.err.println("Usage:<filename><port><millisecond>")
//      System.exit(1)
//    }
//获取指定文件总的行数。
   // val filename = args(0)
    val lines = Source.fromFile("f:/sparktest/a.txt").getLines().toList
    val filerow=lines.length
//指定监听参数args(1)指定的端口，当外部程序请求时建立连接。
    val listener =new ServerSocket(8889.toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run={
          println("Got client connected from:"+socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(1000.toLong)
//当该端口接受请求时，随机获取某行数据发送给对方。
            val content= lines(index(filerow))
            println(content)
            out.write(content+'\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}