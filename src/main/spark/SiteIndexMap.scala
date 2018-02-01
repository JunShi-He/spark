import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.io.Source

/**
  * 将数据中的站点索引映射成站点名称
  */
object SiteIndexMap {
  val inputFile = "C:\\data\\xinshang\\180201\\part-r-00000"
  val hosthcInputFile = "C:\\data\\xinshang\\hosthc.txt"

  // 匹配文件
  val hostMap = new mutable.HashMap[String, String]()
  val file = Source.fromFile(hosthcInputFile)
  file.getLines().foreach(line => {
    hostMap.put(line.split("\\|")(0), line.split("\\|")(1))
  })

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("indexMap").setMaster("local")
    val sc = new SparkContext(conf)
    // 数据文件
    val textFile = sc.textFile(inputFile)

    val data = textFile.map(line => (line.split("\\|")(0), line.split("\\|")(1)))
      .mapValues(
      value => value.split(";")
        .toList.map(getKey).mkString(";")).map(kv => kv._1 + "|" + kv._2);

    data.saveAsTextFile("C:/data/xinshang/180201/out")
//    data.saveAsTextFile()
//    println(getKey("U0:5"))
  }

  def getKey(line:String): String = {
    if(line != null){
      val arr = line.split(":")
      if(arr.length < 2){
        line
      }else{
        if(hostMap.get(arr(0)) == None ){
          line
        }else{
          hostMap.get(arr(0)).get + ":" + arr(1)
        }
      }
    }else{
      line
    }
  }
}