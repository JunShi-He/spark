import SiteIndexMap.inputFile
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by 何俊士 on 2018/2/1.
  *
  * 根据域名过滤用户
  */
object FilterSite {

  val inputFile = "C:\\data\\xinshang\\180201\\out\\"
  val hosthcInputFile = "C:\\data\\xinshang\\180201\\site.csv"

  // 匹配文件
  val hostMap = new mutable.HashSet[String]()
  val file = Source.fromFile(hosthcInputFile)
  file.getLines().foreach(hostMap.add)


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("filterSite").setMaster("local")
    val sc = new SparkContext(conf)
    // 数据文件
    val textFile = sc.textFile(inputFile)

    val data = textFile.map(line => (line.split("\\|")(0), line.split("\\|")(1)))
      .mapValues(line => line.split(";").toList.filter(isCon).mkString(";")).map(kv => kv._1 + "|" + kv._2)
      .saveAsTextFile("data/180201/out")
  }
  def isCon(str:String) : Boolean ={
    for(line <- hostMap){
      if(str.contains(line)){
        return true
      }
    }
    return false
  }
}
