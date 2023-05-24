package com.qcc.es;

import java.time.Duration;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

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
}
