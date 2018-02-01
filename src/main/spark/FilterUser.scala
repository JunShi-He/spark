import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by 何俊士 on 2018/1/29.
  *
  * 分离数据
  */
object FilterUser {



  def main(args: Array[String]): Unit = {
    // 输入数据为映射过的文件
    val inputPath = "C:\\data\\xinshang\\out\\"
    val userConf = "C:\\data\\xinshang\\user_conf.txt"
    val outputFile = "C:\\data\\xinshang\\out1"

    val conf = new SparkConf().setAppName("FilterUser").setMaster("local")
    val sc = new SparkContext(conf)

    // 数据文件
    val textFile = sc.textFile(inputPath)

    val file = Source.fromFile(userConf)
    val users = file.getLines().toArray
    textFile.filter(line =>
      !users.contains(line.split("\\|")(0))
    ).saveAsTextFile(outputFile)
  }
}
