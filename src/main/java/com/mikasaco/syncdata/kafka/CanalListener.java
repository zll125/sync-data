package com.mikasaco.syncdata.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mikasaco.syncdata.elasticsearch.EsClient;
import com.mikasaco.syncdata.enums.DmlTypeEnum;
import com.mikasaco.syncdata.model.CanalMessage;
import com.mikasaco.syncdata.model.ModelTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CanalListener {

    @Autowired
    private EsClient esClient;

    @KafkaListener(topics = "syncdata")
    public void listenCaseInfo(String message) throws Exception {
        log.info("接收到的消息: " + message);

        CanalMessage canalMessage = JSONObject.parseObject(message, CanalMessage.class);
        String data = canalMessage.getData();
        List<String> list = this.jsonArray2ListString(data);

        for (String str : list) {
            ModelTemplate template = JSONObject.parseObject(str, ModelTemplate.class);
            String id = template.getId();
            String table = canalMessage.getTable();
            if (DmlTypeEnum.INSERT.name().equals(canalMessage.getType())) {
                esClient.addDocByJson(table, id, str);
            }
            if (DmlTypeEnum.UPDATE.name().equals(canalMessage.getType())) {
                esClient.updateDocByJson(table, id, str);
            }
            if (DmlTypeEnum.DELETE.name().equals(canalMessage.getType())) {
                esClient.deleteDocument(table, id);
            }
        }
    }

    /**
     * json数组字符串转成字符串list
     * 例如： [{"id":1,"name":"mikasa"},{"id":2,"name":"allen"}]  ==> List第一个元素 {"id":1,"name":"mikasa"},第二个元素 {"id":2,"name":"allen"}
     * 注意！ 不能在内容中出现{或},例如: [{"id":1,"name":"{mik}asa"},{"id":2,"name":"allen"}]
     *
     * @param jsonStr
     * @return
     */
    private List<String> jsonArray2ListString(String jsonStr) {

        boolean isjson = this.isjson(jsonStr);
        if (!isjson) {
            log.error("不是json字符串，{}", jsonStr);
            return null;
        }
        ArrayList<String> list = new ArrayList<>();

        int i = jsonStr.length(), j = i;
        int temp = -1;

        while (i > 0 && j > 0) {
            if ('{' == jsonStr.charAt(--i)) {
                if (temp != -1) {
                    String s = jsonStr.substring(i, temp + 1);
                    list.add(s);
                    temp = -1;
                } else {
                    temp = i;
                }
            }
            if ('}' == jsonStr.charAt(--j)) {
                if (temp != -1) {
                    String s = jsonStr.substring(temp + 1, j);
                    list.add(s);
                    temp = -1;
                } else {
                    temp = j;
                }
            }
        }
        return list;

    }

    /**
     * 判断字符串是否是json格式
     *
     * @param str
     * @return
     */
    private boolean isjson(String str) {
        try {
            JSONArray.parseArray(str);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
