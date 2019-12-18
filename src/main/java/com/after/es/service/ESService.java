package com.after.es.service;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hongjian.chen
 * @date 2019/12/12 16:29
 */

@Service
public class ESService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private RestHighLevelClient client;

    public List getAll() {
        List list = new ArrayList<>();
        //1、构造sourceBuild(source源)
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"first_name", "about", "interests"}, new String[]{}).query(QueryBuilders.matchAllQuery());
        //2、构造查询请求对象
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //3、client 执行查询
        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("error=" + e.getMessage());
        }
        //4、打印结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            list.add(JSON.parseObject(hit.getSourceAsString()));
        }
        return list;
    }
}
