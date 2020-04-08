package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object EntryPoint2 {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """

    case class LogRecord( host: String, timeStamp: String, url:String,httpCode:Int)

    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r
    
    def main(args: Array[String]) {
        
        if (args.length != 3) {
            println("Expected:3 , Provided: " + args.length)
            println(usage)
            return;
        }

        var utils = new Utils

        // Create a local StreamingContext with batch interval of 10 second
        val conf = new SparkConf().setAppName("NASA Access Log File")
        val sc = new SparkContext(conf);
        sc.setLogLevel("WARN")

        val logFile = sc.textFile("/data/spark/project/NASA_access_log_Aug95.gz")
        /*logFile
          .flatMap( line =>

          )*/

        val accessLog = logFile.map(parseLogLine)
        val accessDf = accessLog.toDF()
        accessDf.printSchema
        accessDf.createOrReplaceTempView("nasalog")
        val output = spark.sql("select * from nasalog")
        output.createOrReplaceTempView("nasa_log")
        spark.sql("cache TABLE nasa_log")

        spark.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show

        spark.sql("select host,count(*) as req_cnt from nasa_log group by host order by req_cnt desc LIMIT 5").show

        spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show

        spark.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show

        spark.sql("select httpCode,count(*) as req_cnt from nasa_log group by httpCode ").show

    }

    def parseLogLine(log: String):
        LogRecord = {
            val res = PATTERN.findFirstMatchIn (log)
            if (res.isEmpty) {
                println ("Rejected Log Line: " + log)
                LogRecord ("Empty", "", "", - 1)
            }
            else {
                val m = res.get
                LogRecord (m.group (1), m.group (4), m.group (6), m.group (8).toInt)
            }
        }
}
