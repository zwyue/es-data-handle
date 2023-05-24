package com.qcc.es;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import cn.hutool.core.io.FileUtil;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CatchAllIds {
    static final List<String> ids = new ArrayList();
    static Integer total = 0;

    public CatchAllIds() {
    }

    public static void main(String[] args) {
        RestClient restClient = restClient();
        String scrollId = createScroll(restClient);
        queryByScrollId(scrollId, restClient);
        System.out.println(ids);
        System.exit(0);
    }

    private static RestClient restClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elasticsearchc", "elastic"));
        HttpHost httpHost1 = new HttpHost("172.16.61.21", 11010);
        HttpHost httpHost2 = new HttpHost("172.16.61.22", 11010);
        HttpHost httpHost3 = new HttpHost("172.16.61.23", 11010);
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost[]{httpHost1, httpHost2, httpHost3});
        restClientBuilder.setHttpClientConfigCallback((config) ->
             config.setDefaultCredentialsProvider(credentialsProvider).setKeepAliveStrategy((response, ctx) ->
                 Duration.ofSeconds(120L).getSeconds()
            )
        );
        return restClientBuilder.build();
    }

    private static String createScroll(RestClient restClient) {
        Request request = new Request("POST", "/tender_product_statistics_v3/_search?scroll=1m");
        String query = "{\n  \"_source\": \"id\", \n  \"query\": {\n    \"term\": {\n      \"datastatus\": {\n        \"value\": \"1\"\n      }\n    }\n  },\n  \"size\": 10000\n}";
        request.setJsonEntity(query);

        try {
            Response response = restClient.performRequest(request);
            HttpEntity httpEntity = response.getEntity();
            String result = EntityUtils.toString(httpEntity);
            JSONObject json = JSONObject.parseObject(result);
            JSONObject hits = json.getJSONObject("hits");
            total = total == 0 ? hits.getInteger("total") : total;
            JSONArray hitsArray = hits.getJSONArray("hits");
            Iterator var9 = hitsArray.iterator();

            while(var9.hasNext()) {
                Object o = var9.next();
                JSONObject oj = (JSONObject)o;
                ids.add(oj.getString("_id"));
            }

            return json.getString("_scroll_id");
        } catch (IOException var12) {
            var12.printStackTrace();
            return " ;";
        }
    }

    private static void queryByScrollId(String scrollId, RestClient restClient) {
        Request request = new Request("POST", "/_search/scroll");
        String query = "{\n  \"scroll\" : \"1m\",\n  \"scroll_id\" : \"" + scrollId + "\"\n}";
        request.setJsonEntity(query);

        try {
            Response response = restClient.performRequest(request);
            HttpEntity httpEntity = response.getEntity();
            String result = EntityUtils.toString(httpEntity);
            JSONObject json = JSONObject.parseObject(result);
            JSONObject hits = json.getJSONObject("hits");
            JSONArray hitsArray = hits.getJSONArray("hits");
            Iterator var10 = hitsArray.iterator();

            while(var10.hasNext()) {
                Object o = var10.next();
                JSONObject oj = (JSONObject)o;
                ids.add(oj.getString("_id"));
            }

            FileUtil.appendLines(ids, "ids.txt", StandardCharsets.UTF_8);
            ids.clear();

            while(hitsArray.size() > 0) {
                queryByScrollId(scrollId, restClient);
            }
        } catch (IOException var13) {
            var13.printStackTrace();
        }

    }
}
