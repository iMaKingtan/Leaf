package com.sankuai.inf.leaf.server.controller;

import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.server.exception.LeafServerException;
import com.sankuai.inf.leaf.server.exception.NoKeyException;
import com.sankuai.inf.leaf.server.service.SegmentService;
import com.sankuai.inf.leaf.server.service.SnowflakeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LeafController {
    private final static Logger LOGGER = LoggerFactory.getLogger(LeafController.class);

    @Autowired
    private SegmentService segmentService;
    @Autowired
    private SnowflakeService snowflakeService;


    @GetMapping(value = "/api/segment/get/{key}")// TODO: 2022/12/8 这里可以优化为restful风格的 
    public String getSegmentId(@PathVariable("key") String key) {

        return get(key, segmentService.getId(key));
    }

    @GetMapping(value = "/api/snowflake/get/{key}")
    public String getSnowflakeId(@PathVariable("key") String key) {
        return get(key, snowflakeService.getId(key));
    }

    /**
     * 封装返回结果
     * @param key 业务key
     * @param id 获得的id
     * @return
     */
    private String get(@PathVariable("key") String key, Result id) {
        Result result;
        if (key == null || key.isEmpty()) {
            throw new NoKeyException();
        }
        result = id;
        // TODO: 2022/12/8 controller层不应该主线状态码,而是抛出异常交给ControllerAdvice 
        if (result.getStatus().equals(Status.EXCEPTION)) {
            throw new LeafServerException(result.toString());
        }
        return String.valueOf(result.getId());
    }
}
