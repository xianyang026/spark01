import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/*
*
* rdd转换df 第一种方式 反射 样例类
*
*
* */
object TestScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("gggg").setMaster("local[*]").set("spark.testing.memory", "2147480000")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val rdd = sc.textFile("D:\\data\\user.txt")
    val UserRDD = rdd.map(lines => lines.split(" ")).map(p => User(p(0).toInt, p(1), p(2).toInt))
    val df = UserRDD.toDF()
    df.show()








  }

}
//样例类
case class User(id:Int,name:String,age:Int)
