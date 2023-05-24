package com.qcc.es;

import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MigrateTypeData {

    public static void main(String[] args) {
        RestHighLevelClient catchClient = restClient();
        try {
            Map<String, Aggregation> aggregationMap = queryTypes(catchClient);
            writeType(aggregationMap, catchClient);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static RestHighLevelClient restClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "qUwxUCGlugD5HIba7QD"));
        String[] clusterHostAndPort = "es-cn-2r42bu6ek0067j1ud.elasticsearch.aliyuncs.com:9200".split(",");
        HttpHost[] httpHosts = new HttpHost[clusterHostAndPort.length];

        for (int i = 0; i < httpHosts.length; ++i) {
            String[] hostAndPort = clusterHostAndPort[i].split(":");
            httpHosts[i] = new HttpHost(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
        }

        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        restClientBuilder.setHttpClientConfigCallback((config) ->
                config.setDefaultCredentialsProvider(credentialsProvider).setKeepAliveStrategy((response, ctx) ->
                        Duration.ofSeconds(120L).getSeconds()
                )
        );
        return new RestHighLevelClient(restClientBuilder);
    }

    private static Map<String, Aggregation> queryTypes(RestHighLevelClient restClient) throws IOException {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                .aggregation(AggregationBuilders.terms("casereasoncodeone").field("casereasoncodeone").size(1000))
                .aggregation(AggregationBuilders.terms("casereasoncodetwo").field("casereasoncodetwo").size(1000))
                .aggregation(AggregationBuilders.terms("casereasoncodethree").field("casereasoncodethree").size(1000))
                .aggregation(AggregationBuilders.terms("casereasoncodefour").field("casereasoncodefour").size(1000))
                .aggregation(AggregationBuilders.terms("casereasoncodefive").field("casereasoncodefive").size(1000))
                .aggregation(AggregationBuilders.terms("courtcodeone").field("courtcodeone").size(1000))
                .aggregation(AggregationBuilders.terms("courtcodetwo").field("courtcodetwo").size(1000))
                .aggregation(AggregationBuilders.terms("courtcodethree").field("courtcodethree").size(1000))
                .aggregation(AggregationBuilders.terms("courtcodefour").field("courtcodefour").size(1000))
                .aggregation(AggregationBuilders.terms("trialroundcodeone").field("trialroundcodeone").size(1000))
                .aggregation(AggregationBuilders.terms("trialroundcodetwo").field("trialroundcodetwo").size(1000))
                .aggregation(AggregationBuilders.terms("trialroundcodethree").field("trialroundcodethree").size(1000))
        ;

        SearchRequest searchRequest = new SearchRequest("risk_law_authority_query");
        searchRequest.source(searchSourceBuilder);


        SearchResponse response = restClient.search(searchRequest, RequestOptions.DEFAULT);
        return response.getAggregations().getAsMap();
    }

    private static void writeType(Map<String, Aggregation> aggregationMap, RestHighLevelClient catchClient) throws IOException {

        BulkRequest bulkRequest = new BulkRequest();
        for (String s : aggregationMap.keySet()) {
            Aggregation aggregation = aggregationMap.get(s);
            ParsedStringTerms parsedStringTerms = (ParsedStringTerms) aggregation;
            List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();

            if (buckets.size() > 0) {
                buckets.forEach(bucket -> {
                    String key = (String) bucket.getKey();
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("name",s) ;
                    jsonObject.put("type","dicType") ;
                    bulkRequest.add(new IndexRequest("risk_law_authority_cnt_write").id(key).source(jsonObject));
                });
            }
        }

        catchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        catchClient.close();
    }

}
