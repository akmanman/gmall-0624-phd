package com.atguigu.logger;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ResponseBody
@Slf4j
public class LoggerController {

    @RequestMapping("applog")
    public String logger(@RequestParam("param") String logString){
        // 1.数据落盘
        saveToDisk(logString);

        // 2.写入kafka
        writeToKafka(logString);
        return "ok";
    }
    @Autowired
    KafkaTemplate<String,String> kafka;
    private void writeToKafka(String logString) {
        kafka.send("ods_log",logString);
    }

    private void saveToDisk(String logString) {
        log.info(logString);
    }
}
