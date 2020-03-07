package com.leejean.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.leejean.gmall.common.constant.GmallConstants
import com.leejean.gmall.realtime.bean.EventLog
import com.leejean.gmall.realtime.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

    val sparkconf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkconf, Seconds(5))

    //获取kafka中的数据
    val inputStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
//    inputStream.foreachRDD(
//      rdd => println(rdd.map(_.value()).collect().mkString("\n"))
//    )

    //对kafka中的数据进行转换
    val eventLogStream : DStream[EventLog] = inputStream.map{
      record =>
        //提取json字符串
        val jsonStr:String = record.value()
        //解析字符串，并放入样例类
        val eventLog: EventLog = JSON.parseObject(jsonStr, classOf[EventLog])
        //将日期信息加入数据，便于以后分析
        //解析date
        val date: Date = new Date(eventLog.ts)
        //转换成yyyy-MM-dd HH:mm
        val dataStr = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)
        val dataArr = dataStr.split(" ")
        eventLog.logDate = dataArr(0)
        eventLog.logHour = dataArr(1).split(":")(0)
        eventLog.logHourMinute = dataArr(1)

        eventLog
    }

    //将数据放入redis
    eventLogStream.foreachRDD{//在driver中
            rdd => rdd.foreachPartition{//这里就在excutor中
              //在每个分区中建立连接
              val jedis: Jedis = RedisUtil.getJedisClient
              logItor => {
                for (log <- logItor){
                  //redis中是以键值对为数据的基本结构
                  var key = "dau" + log.logDate
                  val value = log.mid
                  jedis.sadd(key, value)

                }

              }
                jedis.close()
            }


      }






    //在redis中将数据进行过滤
    ssc.start()

    ssc.awaitTermination()


  }

}
