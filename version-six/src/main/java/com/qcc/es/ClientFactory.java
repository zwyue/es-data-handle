package com.qcc.es;

import com.qcc.es.records.EsDataInfo;
import com.qcc.es.records.EsKeyInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.SearchHit;

public class ClientFactory {

    private static final String TYPE = "doc";

    private ClientFactory() {
        throw new IllegalStateException("ClientFactory class");
    }

    public static RestHighLevelClient restClient(EsServer esServer) {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(esServer.getUsername(), esServer.getPassword()));

        String[] clusterHosts = esServer.getHost().split(",");
        HttpHost[] httpHosts = new HttpHost[clusterHosts.length];

        for (int i = 0; i < httpHosts.length; ++i) {
            httpHosts[i] = new HttpHost(clusterHosts[0], esServer.getPort());
        }

        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        restClientBuilder.setHttpClientConfigCallback(config ->
            config.setDefaultCredentialsProvider(credentialsProvider)
                .setKeepAliveStrategy((response, ctx) ->
                    Duration.ofSeconds(120L).getSeconds()
                )
        );
        return new RestHighLevelClient(restClientBuilder);
    }

    public static void doDel(RestHighLevelClient restClient, List<EsKeyInfo> ids, String index) {
        try {
            BulkRequest bulkRequest = new BulkRequest();

            for (EsKeyInfo id : ids) {
                DeleteRequest deleteRequest = new DeleteRequest(index, TYPE, id.esId());
                deleteRequest.version(id.time());
                deleteRequest.versionType(VersionType.EXTERNAL_GTE);
                deleteRequest.routing(id.route());
                bulkRequest.add(deleteRequest);
            }
            restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static EsDataInfo getEsDataInfo(SearchHit hit) {
        String id = hit.getId();
        Map<String, DocumentField> field = hit.getFields();
        DocumentField routingDoc = field.get("_routing");
        String rounting = (String) routingDoc.getValues().get(0);
        Map<String, Object> sourceMap = hit.getSourceAsMap();
        return new EsDataInfo(id, rounting, sourceMap);
    }
}
