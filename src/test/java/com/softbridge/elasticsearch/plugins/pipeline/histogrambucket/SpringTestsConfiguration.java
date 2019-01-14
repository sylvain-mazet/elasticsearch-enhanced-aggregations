/*
 *  Copyright 2018 sOftbridge Technology
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  see the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.softbridge.elasticsearch.plugins.pipeline.histogrambucket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.plugin.EnhancedAggregationsPlugin;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

/**
 * Created by smazet on 24/04/18.
 */
@Configuration
@PropertySource(value = "classpath:application.properties")
public class SpringTestsConfiguration {

    private static final Logger logger = LogManager.getLogger(SpringTestsConfiguration.class);

    @Value("${spring.data.elasticsearch.clusternodes}")
    private String clusterNodes;
    @Value("${spring.data.elasticsearch.clustername}")
    private String clusterName;

    @Bean
    public ElasticsearchTemplate elasticsearchTemplate(Client client) {
        return new ElasticsearchTemplate(client);
    }

    @Bean
    public Client client() {
        // Transport Client
        Settings settings = Settings.builder()
                // Setting "transport.type" enables this module:
                .put("cluster.name", clusterName)
                .put("client.transport.ignore_cluster_name", false)
                //.setSecureSettings(new MockSecureSettings())
                .build();

        // Instantiate a TransportClient and add Found Elasticsearch to the list of addresses to connect to.
        // Only port 9343 (SSL-encrypted) is currently supported.
        TransportClient client = new PreBuiltXPackTransportClient(settings, Collections.singletonList(EnhancedAggregationsPlugin.class));
        for (String clusterNode : clusterNodes.split(",")) {
            try {
                client.addTransportAddress(new TransportAddress(
                        InetAddress.getByName(clusterNode.split(":")[0]), Integer.parseInt(clusterNode.split(":")[1])));
            } catch (UnknownHostException e) {
                logger.error(e.getMessage(), e);
            }
        }

        return client;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
}
