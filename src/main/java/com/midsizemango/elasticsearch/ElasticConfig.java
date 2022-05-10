package com.midsizemango.elasticsearch;

import lombok.Getter;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class ElasticConfig {
    @Value("${elasticsearch.host:localhost}")
    private String host;

    @Value("${elasticsearch.port:9200}")
    private int port;

    private static final int timeout = 60;

    @Bean
    public RestHighLevelClient getRestClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port))
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(timeout * 1000)
                        .setSocketTimeout(timeout * 1000)
                        .setConnectionRequestTimeout(0));

        return new RestHighLevelClient(builder);
    }

}
