package com.wing.es;


import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class HelloEs {

    public static void main(String[] args) throws IOException {



        Settings settings = Settings.builder()
                .put("cluster.name", "es-cluster")
                .put("client.transport.sniff", true)
                .build();

        Client client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.101"), 9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.102"), 9300));

        IndexResponse response = client.prepareIndex("gamelog", "users", "1")
                .setSource(
                        jsonBuilder()
                                .startObject()
                                .field("username", "老赵")
                                .field("gender", "male")
                                .field("birthday", new Date())
                                .field("fv", 9999)
                                .field("message", "trying out Elasticsearch")
                                .endObject()
                ).get();

        System.out.println(response);

        client.close();


    }
}
