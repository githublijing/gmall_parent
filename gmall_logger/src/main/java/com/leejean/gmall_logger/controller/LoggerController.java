package com.leejean.gmall_logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

/**
 * 接受输出的日志
 */
@RestController
public class LoggerController {

    //配置kafka
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class);
    @PostMapping("/log")
    public String dolog(@RequestParam("log") String logJson){

        //{"area":"shanghai","uid":"180","itemid":7,"npgid":34,"evid":"clickItem","os":"ios","pgid":40,"appid":"gmall1205","mid":"mid_284","type":"event"}
        //在此之前已经用log4j替换springboot自带的日志，并编辑日志存放位置
        //1.添加时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());
        //2.日志落地
        logger.info(jsonObject.toJSONString());
        //3.传入kafka
        if (jsonObject.get("type").equals("startup")){
            kafkaTemplate.send("GMALL_STARTUP", jsonObject.toJSONString());
        }else {
            kafkaTemplate.send("GMALL_EVENT", jsonObject.toJSONString());
        }

        return "success";
    }

}
