package com.qcc.es;

import static com.qcc.es.ClientFactory.restClient;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

public class MigrateData {
    static Integer total = 0;
    static Integer sendTimes = 0;
    static boolean recurrences = true;
    static BulkProcessor processor;

    public MigrateData() {
    }

    public static void main(String[] args) {
        RestHighLevelClient catchClient =
            ClientFactory.restClient("es-003.ld-hadoop.com:9200", "elastic", "kq1OsG6SJiOQwwGvUVj6");
        RestHighLevelClient sentClient =
            ClientFactory.restClient("172.16.61.36:10420", "liushuai", "Liushuai123456");
        String scrollId = createScroll(catchClient, sentClient);
        if (scrollId != null) {
            while (recurrences) {
                queryByScrollId(scrollId, catchClient, sentClient);
            }
        }

        processor.close();
        System.exit(0);
    }


    private static String createScroll(RestHighLevelClient restClient,
                                       RestHighLevelClient sentClient) {
        Request request = new Request("POST", "/clean_news_xingguang_url/_search?scroll=5h");
        String query = "{\n  \"query\": {\n    \"match_all\": {}\n  },\n  \"size\": 8000\n}";
        request.setJsonEntity(query);

        try {
            Response response = restClient.getLowLevelClient().performRequest(request);
            HttpEntity httpEntity = response.getEntity();
            String result = EntityUtils.toString(httpEntity);
            JSONObject json = JSONObject.parseObject(result);
            JSONObject hits = json.getJSONObject("hits");
            total = total == 0 ? hits.getInteger("total") : total;
            if (total == 0) {
                recurrences = false;
                return null;
            } else {
                System.out.println("....... doc count: " + total);
                JSONArray hitsArray = hits.getJSONArray("hits");
                sentData(hitsArray, sentClient);
                System.out.println(json.getString("_scroll_id"));
                return json.getString("_scroll_id");
            }
        } catch (IOException var10) {
            var10.printStackTrace();
            return null;
        }
    }

    private static void queryByScrollId(String scrollId, RestHighLevelClient restClient,
                                        RestHighLevelClient sentClient) {
        Request request = new Request("POST", "/_search/scroll");
        String query = "{\n  \"scroll\" : \"1h\",\n  \"scroll_id\" : \"" + scrollId + "\"\n}";
        request.setJsonEntity(query);

        try {
            Response response = restClient.getLowLevelClient().performRequest(request);
            HttpEntity httpEntity = response.getEntity();
            String result = EntityUtils.toString(httpEntity);
            JSONObject json = JSONObject.parseObject(result);
            JSONObject hits = json.getJSONObject("hits");
            JSONArray hitsArray = hits.getJSONArray("hits");
            if (hitsArray.size() > 0) {
                sentData(hitsArray, sentClient);
            } else {
                recurrences = false;
            }
        } catch (IOException var11) {
            var11.printStackTrace();
            System.out.println("scrollId:" + scrollId);
        }

    }

    public static void sentData(JSONArray hitsArray, RestHighLevelClient sentClient)
        throws IOException {
        try {
            BulkProcessor bulkProcessor = getBulkProcessor(sentClient);
            Iterator var3 = hitsArray.iterator();

            while (var3.hasNext()) {
                Object o = var3.next();
                JSONObject oj = (JSONObject) o;
                IndexRequest indexRequest = new IndexRequest();
                indexRequest.index("clean_news_xingguang_url_v2");
                indexRequest.id(oj.getString("_id"));
                indexRequest.source(oj.getJSONObject("_source"));
                bulkProcessor.add(indexRequest);
            }
        } catch (Exception var7) {
            System.out.println("...... 鍐欏叆閿欒\ue1e4 ......" + var7.getMessage());
        }

        PrintStream var10000 = System.out;
        StringBuilder var10001 = (new StringBuilder()).append("....... sent data : ");
        Integer var8 = sendTimes;
        sendTimes = sendTimes + 1;
        var10000.println(var10001.append(var8));
    }

    public static BulkProcessor getBulkProcessor(final RestHighLevelClient sentClient) {
        if (processor != null) {
            return processor;
        } else {
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                public void beforeBulk(long executionId, BulkRequest request) {
                }

                public void afterBulk(long executionId, BulkRequest request,
                                      BulkResponse response) {
                    if (response.hasFailures()) {
                        System.out.println("Bulk " + executionId + " executed with failures");
                    }

                }

                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    System.out.println("Failed to execute bulk " + failure);

                    try {
                        sentClient.bulk(request, RequestOptions.DEFAULT);
                    } catch (IOException var6) {
                        System.out.println("Failed to retry execute bulk " + failure);
                    }

                }
            };
            BulkProcessor.Builder builder = BulkProcessor.builder((request, bulkListener) -> {
                sentClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
            }, listener, "bulk-processor-name");
            builder.setBulkActions(3000);
            builder.setBulkSize(new ByteSizeValue(2L, ByteSizeUnit.MB));
            builder.setConcurrentRequests(4);
            builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
            builder.setBackoffPolicy(
                BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(2L), 5));
            processor = builder.build();
            return processor;
        }
    }
}
