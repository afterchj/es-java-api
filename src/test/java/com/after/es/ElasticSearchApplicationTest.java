package com.after.es;

import com.alibaba.fastjson.JSON;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder.Field;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticSearchApplicationTest {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private RestClient restClient;


    @Autowired
    private RestHighLevelClient client;

    /**
     * 查询type下所有文档
     * 打印结果:
     * {"studymodel":"201002","name":"Bootstrap开发"}
     * {"studymodel":"201001","name":"java编程基础"}
     * {"studymodel":"201001","name":"spring开发基础"}
     * <p>
     * 对应http请求json
     * {
     * "query": {
     * "match_all": {}
     * },
     * "_source": ["name","studymodel"]
     * }
     */
    @Test
    public void testSearchAll() throws Exception {

        List list=new ArrayList();
        //1、构造sourceBuild(source源)
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"first_name", "about","interests"}, new String[]{}).query(QueryBuilders.matchAllQuery());
        //2、构造查询请求对象
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //3、client 执行查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        //4、打印结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            list.add(JSON.parseObject(hit.getSourceAsString()));
            logger.warn("result=" + hit.getSourceAsString());
        }
        logger.warn("jsonObject = "+JSON.toJSONString(list));
    }


    /**
     * 分页查询type下所有文档
     * <p>
     * json 参数
     * {
     * "from":0,
     * "size":1,
     * "query": {
     * "match_all": {}
     * },
     * "_source": ["name","studymodel"]
     * }
     * <p>
     * 打印结果
     * {"studymodel":"201002","name":"Bootstrap开发"}
     * {"studymodel":"201001","name":"java编程基础"}
     */
    @Test
    public void testSearchAllByPage() throws Exception {

        //1、构造sourceBuild
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.fetchSource(new String[]{"first_name", "about"}, new String[]{})
                .query(QueryBuilders.matchAllQuery())
                .from(0).size(2);//分页查询,下表从0开始
        //2、构造searchRequest请求对象
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //3、client执行请求
        SearchResponse searchResponse = client.search(searchRequest);
        //4、打印结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }

    /**
     * term query: 精确查询、在搜索是会精确匹配关键字、搜索关键字不分词
     * <p>
     * json 参数
     * {
     * <p>
     * "query": {
     * "term": {
     * name: "spring"
     * }
     * },
     * "_source": ["name","studymodel"]
     * }
     */
    @Test
    public void testTermQuery() throws Exception {

        //1、设置queryBuilder
        TermQueryBuilder termQueryBuild = QueryBuilders.termQuery("about", "collect");

        //2、设置sourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termQueryBuild)//设置Term query查询
                .fetchSource(new String[]{"first_name", "about"}, new String[]{});
        //3、构造searchRequest
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //4、client发出请求
        SearchResponse searchResponse = client.search(searchRequest);

        //5、打印结果
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
           logger.warn("result = "+hit.getSourceAsString());
        }
    }


    /**
     * 根据id精确查询:根据提供的多个id去匹配
     * <p>
     * json 参数
     * {
     * query{
     * "ids": {
     * "type": "doc",
     * "values": ["TeH_2WcBH5cUK","TuEB2mcBf3IfcTiHccWJ"]
     * }
     * },
     * "_source": ["name","studymodel"]
     * }
     */
    @Test
    public void testIdsQuery() throws Exception {

        //1、够着queryBuild
        //构造idList,注意数组每个元素必须是一个完整的id能匹配的上,第一条没有记录匹配上，第二条中
        String[] idList = new String[]{"1", "2"};
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("_id", idList);//特别注意用termsQuery,不要用termQuery

        //2、构造sourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(termsQueryBuilder).fetchSource(new String[]{"first_name", "about"}, new String[]{});
        //3、构造searchRequest
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //4、client执行
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();

        //5、打印结果
        for (SearchHit hit : hits) {
           logger.warn("result = "+hit.getSourceAsString());
        }
    }


    /**
     * match Query就是全文检索,收缩方式就是先将搜索字符串分词、然后到索引分词列表去匹配
     * json 参数
     * {
     * query{
     * "match": {
     * "descrition":{  //还是需要指定字段的，description是字段名
     * "query": "世界第一",
     * "operate": "or"  //or表示分词之后，只要有一个匹配即可,and表示分词在文档中都匹配才行
     * }
     * },
     * "_source": ["name","studymodel"]
     * }
     * }
     */
    @Test
    public void testmatchQuery() throws Exception {

        //queryBuild
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("about", "like");

        //searchSorcebuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder)
                .fetchSource(new String[]{"last_name", "about"}, new String[]{});


        //searchRequest
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //client->search
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        //print end
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }

    /**
     * minimum_should_match:
     * or只能表示只要匹配一个即可、minimum_should_match可以指定文档匹配词的占比,注意这个占比的基数是搜索字符串分词的个数
     * json 参数
     * {
     * query{
     * "match": {
     * "descrition":{  //还是需要指定字段的，description是字段名
     * "query": "spring开发",
     * "minimun_should_match": "80%"
     * }
     * },
     * "_source": ["name","studymodel"]
     * }
     * }
     */
    @Test
    public void testMinimumShouldMatchQuery() throws Exception {

        //queryBuild
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("first_name", "John").minimumShouldMatch("20%");


        //searchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder)
                .fetchSource(new String[]{"first_name","interests", "about"}, new String[]{});

        //searchRequest
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }

    /**
     * multi_match Query:
     * 用于一次匹配多个File进行全文检索、前面match都是一个Field
     * 多个字段可以通过提升boost(权重),来提高得分,实现排序靠前
     * <p>
     * json 参数
     * {
     * query{
     * "multi_match": {
     * "query": "spring css", //搜索字符串
     * "minimum_should_match": "50%",
     * "fields": ["name^10","description"] //设置匹配name 和 description字段,将boost的boost提10倍
     * }
     * }
     * }
     */
    @Test
    public void testMultiMatchQuery() throws Exception {


        //queryBuilder
        MultiMatchQueryBuilder matchQueryBuilder = QueryBuilders.multiMatchQuery("collect", "first_name", "about")
                .minimumShouldMatch("50%")//设置百分比
                .field("first_name", 10);//提升boost
        //searchSourceBuild
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(matchQueryBuilder);

        //searchRequest
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee").source(searchSourceBuilder);
        //search
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }

    /**
     * 布尔查询
     * 对应lucene的Boolean查询、实现将多个查询条件结合起来
     * 三个参数：
     * must:只有符合所有查询的文档才被查询出来,相当于AND
     * should:至少符合其中一个,相当于OR
     * must_not:不能符合任意查询条件,相当于NOT
     * <p>
     * json 参数
     * {
     * "_source": ["name","pic"],
     * "from": 0,
     * "size": 1,
     * query{
     * bool:{
     * must: [
     * {
     * "multi_match": {
     * "query": "spring框架",
     * "minimum_should_match": "50%",
     * "fields": ["name^10","description"]
     * }
     * },{
     * "term": {
     * "studymodel": "201001"
     * }
     * }
     * ]
     * }
     * }
     * }
     */
    @Test
    public void testBooleanQuery() throws Exception {

        //1、够着QueryBuild

        //构造multiQureyBuilder
        MultiMatchQueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery("collect", "first_name", "about")
                .minimumShouldMatch("50%")//设置百分比
                .field("name", 10);
        //构造termQueryBuilder
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("about", "like");

        //构造booleanQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(multiQueryBuilder)
                .must(termQueryBuilder);

        //2、构造查询源
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.fetchSource(new String[]{"last_name", "interests"}, new String[]{});
        ssb.query(boolQueryBuilder);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee");
        searchRequest.source(ssb);

        //4、client执行查询
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }


    /**
     * 过滤器:
     * 过滤器判断的是文档是否匹配，不去计算和判断文档的匹配度得分,所以过滤器性能比查询高、方便缓存
     * 推荐尽量使用过滤器、或则过滤器搭配查询使用
     * 过滤器使用的前提是bool查询
     * 过滤器可以单独使用,但是不能提点multi Query, 因为过滤器每个Query都是单字段过滤
     * <p>
     * json 参数
     * {
     * "_source": ["name","pic"],
     * "from": 0,
     * "size": 1,
     * query{
     * bool:{
     * must: [
     * {
     * "multi_match": {
     * "query": "spring框架",
     * "minimum_should_match": "50%",
     * "fields": ["name^10","description"]
     * }
     * }
     * ],
     * fileter: [
     * {
     * term: {"studymodel": "21001"} //针对字段进行过滤
     * },{
     * range: {  //针对范围进行过滤
     * "price": {"gte":60,"lte":100}
     * }
     * }
     * ]
     * }
     * }
     * }
     */
    @Test
    public void testFileter() throws Exception {

        //1、构造QueryBuild

        //构造multiQureyBuilder
        MultiMatchQueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery("collect", "about")
                .minimumShouldMatch("50%")//设置百分比
                .field("about", 10);

        //构造booleanQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(multiQueryBuilder);

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("about", "like"))
                .filter(QueryBuilders.rangeQuery("age").gte(20).lte(100));

        //2、构造查询源
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.fetchSource(new String[]{"last_name","about", "age"}, new String[]{});
        ssb.query(boolQueryBuilder);

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee");
        searchRequest.source(ssb);

        //4、client执行查询
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            logger.warn("result = "+hit.getSourceAsString());
        }
    }


    /**
     * 排序:
     * 可以设置排序字段对查询结果进行排序
     * keyword 、date 、float 等可以加
     * 注意text 不能加
     * <p>
     * json 参数
     * {
     * "_source": ["name","pic","description","price],
     * query{
     * bool:{
     * fileter: [ //过滤器也可以单独使用,但是只能用于单个字段
     * {
     * term: {"studymodel": "21001"} //针对字段进行过滤
     * },{
     * range: {  //针对范围进行过滤
     * "price": {"gte":60,"lte":100}
     * }
     * }
     * ]
     * }
     * },
     * "sort": [
     * {
     * "studymodel": "desc"
     * },{
     * "price": "asc"
     * }
     * ]
     * }
     */
    @Test
    public void testSort() throws Exception {

        //1、构造QueryBuild

        //构造booleanQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("about", "like"))
                .filter(QueryBuilders.rangeQuery("age").gte(20).lte(100));

        //2、构造查询源
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.fetchSource(new String[]{"last_name", "about", "age", "interests"}, new String[]{});
        ssb.query(boolQueryBuilder);
        ssb.sort(new FieldSortBuilder("age").order(SortOrder.DESC));

        //3、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee");
        searchRequest.source(ssb);

        //4、client执行查询
        SearchResponse searchResponse = client.search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
           logger.warn("result = "+hit.getSourceAsString());
        }
    }


    /**
     * 高亮显示:
     * 将搜索结果中的一个或多个字突出显示，以便向用户展示匹配的关键字的位置
     * <p>
     * json 参数
     */
    @Test
    public void testHighlight() throws Exception {

        //1、构造QueryBuild

        MultiMatchQueryBuilder multiQueryBuilder = QueryBuilders.multiMatchQuery("like", "about", "interests")
                .field("about", 10)
                .minimumShouldMatch("50%");

        //构造booleanQueryBuilder
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(multiQueryBuilder);

        //过滤
        boolQueryBuilder.filter(QueryBuilders.termQuery("about", "like"))
                .filter(QueryBuilders.rangeQuery("age").gte(20).lte(100));

        //2、设置高亮
        //设置标签
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<tag>")//设置签缀
                .postTags("</tag>");//设置后缀
        //设置高亮字段
        highlightBuilder.fields().add(new HighlightBuilder.Field("about"));
        highlightBuilder.fields().add(new Field("interests"));


        //3、构造查询源
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        ssb.fetchSource(new String[]{"first_name", "age", "about"}, new String[]{})
                .query(boolQueryBuilder)
                .sort(new FieldSortBuilder("age").order(SortOrder.ASC))
                .highlighter(highlightBuilder);

        //4、构造请求对象查询
        SearchRequest searchRequest = new SearchRequest("megacorp");
        searchRequest.types("employee")
                .source(ssb);

        //5、client执行查询
        SearchResponse searchResponse = client.search(searchRequest);
        //6、取出高亮字段
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
           logger.warn("result = "+hit.getHighlightFields());
        }
    }

    //添加文档
    @Test
    public void testAddDoc() throws Exception {

        String path = this.getClass().getClassLoader().getResource("mapping.json").getPath();
        String str = MainTest.readJsonFile(path);
        //设置映射
        System.out.println("path=" + path + ",mappingJson=" + str);
        //获取索引库对象
        IndexRequest indexRequest = new IndexRequest("megacorp", "employee","4");
        indexRequest.source(str, XContentType.JSON);
        //往索引库添加文档,这个动作也叫索引
        IndexResponse indexResponse = client.index(indexRequest);
        //打印结果
        System.out.println("result=" + indexResponse.getResult());
    }

    /**
     * 查询文档(根据id查)
     * 结果
     * {
     * "description":"Bootstrap是由Twitter推出的一个前台页面开发框架，在行业之中使用较为广泛。
     * 此开发框架包 含了大量的CSS、JS程序代码，可以帮助开发者（尤其是不擅长页面开发的程序人员）
     * 轻松的实现一个不受浏览器限制的 精美界面效果。",
     * "name":"Bootstrap开发框架",
     * "studymodel":"201001",
     * "price":62.658
     * }
     */

    @Test
    public void testGetDoc() throws Exception {

        GetRequest getRequest = new GetRequest("website", "blog", "123");
        GetResponse getResponse = client.get(getRequest);
        if (getResponse.isExists()) {
            String sourceAsString = getResponse.getSourceAsString();
            System.out.println("sourceAsString=" + sourceAsString);
        }
    }

    /**
     * 更新文档
     * 打印结果： OK
     * 注意这里采用的是局部更新：只修改map中设置的字段，没有的不会更新。
     * 更新文档的实际顺序是： 检索文档、标记删除、创建新文档、删除原文档
     * 创建新文档就会重构索引(分词-重构倒排索引树)
     */
    @Test
    public void testUpdateDoc() throws Exception {

        UpdateRequest updateRequest = new UpdateRequest("megacorp", "employee", "3");
        Map map = new HashMap();
        map.put("first_name", "steven");
        map.put("last_name", "job");
        map.put("age", 54);
        map.put("about", "I love programming.");
        map.put("interests", new String[]{"sport","book"});
        updateRequest.doc(map);
        UpdateResponse updateResponse = client.update(updateRequest);
       logger.warn("status=" + updateResponse.status());
    }

    /**
     * 删除文档
     * 打印结果：DELETED
     */
    @Test
    public void testDelDoc() throws Exception {
        DeleteRequest deleteRequest = new DeleteRequest("xc_course", "doc", "mI3Z9G4BoWg8tzsKLGFj");
        DeleteResponse deleteResponse = client.delete(deleteRequest);
        System.out.println("result=" + deleteResponse.getResult());
    }

}