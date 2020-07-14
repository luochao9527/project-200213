package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class EsReader {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.创建配置信息
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder
                                    ("http://hadoop102:9020").build();
        //3.设置配置信息
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //4.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        //5.创建Search对象
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"male\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favo\": \"球\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();

        //6.执行查询操作
        SearchResult searchResult = jestClient.execute(search);

        //7.解析searchResult
        //获取总数
        System.out.println("总数:" + searchResult.getTotal() + "条!");
        //获取最高分
        System.out.println("最高分:" + searchResult.getMaxScore());
        //获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
      for ( SearchResult.Hit<Map, Void> hit : hits ){
          Map source = hit.source;
          for (Object key : source.keySet()){
              System.out.println("Key: " + key + ",Value:" + source.get(key));
          }
          System.out.println("******************");
      }

      //获取聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation group_by_sex = aggregations.getTermsAggregation("group_by_sex");
        for (TermsAggregation.Entry entry : group_by_sex.getBuckets()) {
            System.out.println("Key:" + entry.getKey() + ",Count:" + entry.getCount());
            System.out.println("***********************");
        }

        //关闭资源
        jestClient.close();

    }
}
