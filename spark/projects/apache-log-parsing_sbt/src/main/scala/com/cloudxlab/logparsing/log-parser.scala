package com.cloudxlab.logparsing

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class Utils extends Serializable {
    val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)(.*)" (\d{3}) (\S+)""".r

    def containsIP(line:String):Boolean = return line matches "^([0-9\\.]+) .*$"

    def containsURL(line: String) : Boolean = return line matches "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]"

    //Extract only IP
    def extractIP(line:String):(String) = {
        val pattern = "^([0-9\\.]+) .*$".r
        val pattern(ip:String) = line
        return (ip.toString)
    }

    // Extract only URL
    def extractURL(line:String):(String) = {
        val pattern = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]".r
        val pattern(url:String) = line
        return (url.toString)
    }

    def gettop10(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have IP
        var ipaccesslogs = accessLogs.filter(containsIP)
        //var cleanips = ipaccesslogs.map(extractIP(_))
        var cleanips = ipaccesslogs.map(extractIP(_)).filter(isClassA)
        var ips_tuples = cleanips.map((_,1));
        var frequencies = ips_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }

    def isClassA(ip:String):Boolean = {
        ip.split('.')(0).toInt < 127
    }

    def getTopNURLs(accessLogs:RDD[String], sc:SparkContext, topn:Int):Array[(String,Int)] = {
        //Keep only the lines which have URLs
        var urlaccesslogs = accessLogs.filter(containsURL)
        var cleanips = urlaccesslogs.map(extractURL(_))
        var url_tuples = cleanips.map((_,1));
        var frequencies = url_tuples.reduceByKey(_ + _);
        var sortedfrequencies = frequencies.sortBy(x => x._2, false)
        return sortedfrequencies.take(topn)
    }
}

object EntryPoint {
    val usage = """
        Usage: EntryPoint <how_many> <file_or_directory_in_hdfs>
        Eample: EntryPoint 10 /data/spark/project/access/access.log.45.gz
    """
    
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

        // var accessLogs = sc.textFile("/data/spark/project/access/access.log.45.gz")
        var accessLogs = sc.textFile(args(2))
        val top10 = utils.gettop10(accessLogs, sc, args(1).toInt)
        /*println("===== TOP 10 IP Addresses =====")
        for(i <- top10){
            println(i)
        }*/

        val topNURLs = utils.gettop10(accessLogs, sc, args(1).toInt)
        println("===== TOP 10 URLs =====")
        for(url <- topNURLs){
            println(url)
        }
    }
}
