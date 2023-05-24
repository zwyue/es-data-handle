package com.qcc.es;

import com.qcc.es.records.EsKeyInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.VersionType;

public class ClientFactory {

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

    public static void doDel(RestHighLevelClient restClient, List<EsKeyInfo> ids) throws
        IOException {

        BulkRequest bulkRequest = new BulkRequest();

        for (EsKeyInfo id : ids) {
            DeleteRequest deleteRequest =
                new DeleteRequest("in_type_building_write_new", "doc", id.esId());
            deleteRequest.version(id.time()) ;
            deleteRequest.versionType(VersionType.EXTERNAL_GTE) ;
            deleteRequest.routing(id.route()) ;
            bulkRequest.add(deleteRequest);
        }
        BulkResponse response = restClient.bulk(bulkRequest, RequestOptions.DEFAULT);
    }
}
