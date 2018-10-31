package com.wing.es;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class HelloEsCURD {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        Settings settings = Settings.builder()
                .put("cluster.name", "es-cluster")
                .put("client.transport.sniff", true)
                .build();

        Client client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.101"), 9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.102"), 9300));

        //create(client);

        //get(client);

        //gets(client);

        //update(client);

        //delete(client);

        //deleteForFilter(client);

        //testDeleteByQueryAsync(client);


        //range(client);

        //testAgg1(client);

        //testAgg2(client);

        //queryByAddr(client);

        //mutiQuery(client);

        //regQuery(client);

        //CombiningSearch(client);

        //max(client);

        //maxAvg(client);

        client.close();

    }

    private static void maxAvg(Client client) {
        SearchRequestBuilder builder = client.prepareSearch("player_info").setTypes("player");
        //指定分组字段
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms("team_name").field("team");
        //指定聚合函数是求平均数据
        AvgAggregationBuilder avgAgg = AggregationBuilders.avg("avg_age").field("age");
        //指定另外一个聚合函数是求和
        SumAggregationBuilder sumAgg = AggregationBuilders.sum("total_salary").field("salary");
        //分组的聚合器关联了两个聚合函数
        builder.addAggregation(termsAgg.subAggregation(avgAgg).subAggregation(sumAgg));
        SearchResponse response = builder.execute().actionGet();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        //按分组的名字取出数据
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        for (Terms.Bucket teamBucket : teams.getBuckets()) {
            //获取球队名字
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            //根据别名取出平均年龄
            InternalAvg avgAge = (InternalAvg)subAggMap.get("avg_age");
            //根据别名取出薪水总和
            InternalSum totalSalary = (InternalSum)subAggMap.get("total_salary");
            double avgAgeValue = avgAge.getValue();
            double totalSalaryValue = totalSalary.getValue();
            System.out.println(team + " " + avgAgeValue + " " + totalSalaryValue);
        }
    }

    private static void max(Client client) throws InterruptedException, ExecutionException {
        SearchRequestBuilder search = client.prepareSearch("player_info").setTypes("player");
        TermsAggregationBuilder teamAggregationBuilder = AggregationBuilders.terms("team_name").field("team");
        MaxAggregationBuilder maxAgeAggregationBuilder = AggregationBuilders.max("max_age").field("age");
        search.addAggregation(teamAggregationBuilder.subAggregation(maxAgeAggregationBuilder));
        SearchResponse response = search.execute().get();
        Map<String, Aggregation> aggMap = response.getAggregations().getAsMap();
        StringTerms teams = (StringTerms) aggMap.get("team_name");
        teams.getBuckets().forEach(teamBucket -> {
            String team = (String) teamBucket.getKey();
            Map<String, Aggregation> subAggMap = teamBucket.getAggregations().getAsMap();
            InternalMax ages = (InternalMax) subAggMap.get("max_age");
            System.out.println(team + "," + ages.getValue());
        });
    }

    private static void CombiningSearch(Client client) {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("team", "hou"))
                .must(QueryBuilders.rangeQuery("age").from(33).includeLower(true))
                .filter(QueryBuilders.termQuery("addr", "中国"));

        SearchResponse response = client.prepareSearch("player_info")
                .addSort("name", SortOrder.ASC)
                .setTypes("player")
                .setQuery(queryBuilder)
                .get();

        System.out.println("length: " + response.getHits().totalHits);
        if(response.getHits().totalHits > 0) {
            response.getHits().forEach(hit -> {
                System.out.println(hit.getScore() + " --> " + hit.getSourceAsString());
            });
        }
    }

    private static void regQuery(Client client) {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("name", "*o*");


        SearchResponse response = client.prepareSearch("player_info")
                .setTypes("player")
                .setQuery(queryBuilder)
                .get();
        System.out.println("length: " + response.getHits().totalHits);
        if(response.getHits().totalHits > 0) {

            response.getHits().forEach(hit -> {
                System.out.println(hit.getScore() + " --> " + hit.getSourceAsString());
            });
        }
    }

    private static void mutiQuery(Client client) {
        QueryBuilder queryBuilder = QueryBuilders.multiMatchQuery("国际", "addr", "description");
        SearchResponse response = client.prepareSearch("player_info")
                .setTypes("player")
                .setQuery(queryBuilder)
                .get();
        System.out.println("length: " + response.getHits().totalHits);
        if(response.getHits().totalHits > 0) {

            response.getHits().forEach(hit -> {
                System.out.println(hit.getScore() + " --> " + hit.getSourceAsString());
            });
        }
    }

    private static void queryByAddr(Client client) {
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("addr", "闸北区");

        SearchResponse response = client.prepareSearch("player_info")
                .setTypes("player")
                .setQuery(queryBuilder)
                .get();
        System.out.println("length: " + response.getHits().getHits().length);
        if (response.getHits().getTotalHits() != 0) {
            for (SearchHit hit : response.getHits().getHits()) {
                System.out.println(hit.getScore() + " --> " + hit.getSourceAsString());
            }

        }
    }

    private static void testAgg2(Client client) throws InterruptedException, ExecutionException {
        SearchRequestBuilder search = client.prepareSearch("player_info").setTypes("player");
        TermsAggregationBuilder parentTerm = AggregationBuilders.terms("player_count").field("team");
        TermsAggregationBuilder subTerm = AggregationBuilders.terms("position_count").field("position");
        search.addAggregation(parentTerm.subAggregation(subTerm));
        SearchResponse response = search.execute().get();
        Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        StringTerms terms = (StringTerms) aggMap.get("player_count");
        terms.getBuckets().forEach(bucket -> {
            String teamName = (String) bucket.getKey();
            Map<String, Aggregation> positionCountMap = bucket.getAggregations().getAsMap();
            StringTerms postitionTerms = (StringTerms) positionCountMap.get("position_count");
            postitionTerms.getBuckets().forEach(b -> {
                System.out.println(teamName + "," + b.getKey() + "," + b.getDocCount());
            });
        });
    }

    private static void testAgg1(Client client) throws InterruptedException, ExecutionException {
        SearchRequestBuilder search = client.prepareSearch("player_info").setTypes("player");

        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("player_count").field("team");

        search.addAggregation(aggregationBuilder);

        SearchResponse response = search.execute().get();

        Map<String, Aggregation> aggMap = response.getAggregations().asMap();

//        Set<String> keys = aggMap.keySet();
//
//        for (String key : keys) {
//            System.out.println(key);
//        }

        StringTerms terms = (StringTerms) aggMap.get("player_count");

        terms.getBuckets().forEach(bucket -> {
            String team = (String) bucket.getKey();
            long count = bucket.getDocCount();
            System.out.println(team + "," + count);
        });
    }

    private static void range(Client client) {
        QueryBuilder qb = rangeQuery("fv")
                // [88.99, 10000)
                .from(8889)
                .to(9999)
                .includeLower(true)
                .includeUpper(true);

        SearchResponse response = client.prepareSearch("gamelog").setQuery(qb).get();

        System.out.println(response);
    }


    private static void testDeleteByQueryAsync(Client client) {
        DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(matchQuery("gender", "male"))
                .source("gamelog")
                .execute(new ActionListener<BulkByScrollResponse>() {
                    @Override
                    public void onResponse(BulkByScrollResponse response) {
                        long deleted = response.getDeleted();
                        System.out.println("数据删除了");
                        System.out.println(deleted);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        e.printStackTrace();
                    }
                });

    }

    private static void deleteForFilter(Client client) {
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        //指定查询条件
                        .filter(matchQuery("username", "老段"))
                        //指定索引名称
                        .source("gamelog")
                        .get();

        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    private static void delete(Client client) {
        DeleteResponse response = client.prepareDelete("gamelog", "users", "2").get();
        System.out.println(response.status());
    }

    private static void update(Client client) throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("gamelog");
        updateRequest.type("users");
        updateRequest.id("2");
        updateRequest.doc(
                jsonBuilder()
                        .startObject()
                        .field("fv", 999.9)
                        .endObject());
        client.update(updateRequest).get();
    }

    private static void gets(Client client) {
        MultiGetResponse multiGetResponse = client.prepareMultiGet()
                .add("gamelog", "users", "1")
                .add("gamelog", "users", "2")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                System.out.println(response.getSourceAsString());
            }
        }
    }

    private static void get(Client client) {
        GetResponse response = client.prepareGet("gamelog", "users", "1").get();
        System.out.println(response.getSourceAsString());
    }

    private static void create(Client client) throws IOException {
        IndexResponse response = client.prepareIndex("gamelog", "users", "2")
                .setSource(
                        jsonBuilder()
                                .startObject()
                                .field("username", "老段")
                                .field("gender", "male")
                                .field("birthday", new Date())
                                .field("fv", 8888)
                                .field("message", "trying out Elasticsearch")
                                .endObject()
                ).get();

        System.out.println(response.getIndex());
    }
}
