import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SayHello {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wode").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("D:\\data\\user.txt")
    rdd.collect().foreach(println)

    println("hello,word!")
  }

}
