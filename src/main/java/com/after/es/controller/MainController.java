package com.after.es.controller;

import com.after.es.service.ESService;
import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author hongjian.chen
 * @date 2019/12/12 16:25
 */

@RestController
public class MainController {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ESService esService;

    @RequestMapping("/show")
    public List getAll() {
        List list = esService.getAll();
        logger.warn("jsonObject {} ", JSON.toJSONString(list));
        return list;
    }
}
