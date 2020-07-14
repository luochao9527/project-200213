package com.atguigu.reader;

import com.sun.org.apache.bcel.internal.generic.NEW;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReaderByJava {
    public static void main(String[] args) throws IOException {

        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建配置信息
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置配置信息
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.创建DSL语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //定义全值匹配过滤器
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        boolQueryBuilder.filter(termQueryBuilder);

        //定义分词匹配过滤器
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favo", "球");
        boolQueryBuilder.must(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        //6.创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex("student")
                .addType("_doc").build();

        //7.执行查询操作
        SearchResult searchResult = jestClient.execute(search);

        //8.解析searchResult
        //获取总数
        System.out.println("总数:" + searchResult.getTotal() + "条！");
        //获取最高分
        System.out.println("最高分:" + searchResult.getMaxScore());
        //获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
       for( SearchResult.Hit<Map, Void> hit : hits ){
          Map source = hit.source;
         for  (Object key : source.keySet()) {
             System.out.println("Key:" + key + ",Value:" + source.get(key));
           }
           System.out.println("********************");
       }
        //9.关闭资源
        jestClient.close();
    }

}
