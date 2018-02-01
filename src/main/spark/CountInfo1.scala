import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 何俊士 on 2018/1/29.
  * 统计部分信息
  */
object CountInfo1 {
  // 输入数据为映射过的文件
  val inputPath = "C:\\data\\xinshang\\out1";

  val outputPath = "C:\\data\\xinshang\\out3";

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("counterInfo1").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputPath);

    textFile.map(line => (line.split("\\|")(0),line.split("\\|")(1).split(";").map(pv => pv.split(":")(0))))
      .flatMap(a => a._2.map(line => (line,1)))
      .reduceByKey((a,b) => a+b)
        .sortBy(a=>a._2)

      .saveAsTextFile(outputPath)
  }

}
