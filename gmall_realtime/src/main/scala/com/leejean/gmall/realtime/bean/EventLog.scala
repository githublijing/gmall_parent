package com.leejean.gmall.realtime.bean

case class EventLog(mid:String,
                    uid:String,
                    appid:String,
                    area:String,
                    os:String,
                    logType:String,

                    var logDate:String,
                    var logHour:String,
                    var logHourMinute:String,
                    var ts:Long){



}
