package com.wing.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

public class AdminOp {

    public static void main(String[] args) throws IOException {

        Settings settings = Settings.builder()
                .put("cluster.name", "es-cluster")
                .put("client.transport.sniff", true)
                .build();

        Client client = new PreBuiltTransportClient(settings).addTransportAddresses(
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.101"), 9300),
                new InetSocketTransportAddress(InetAddress.getByName("192.168.56.102"), 9300));

        Map<String, Object> settings_map = new HashMap<String, Object>(2);
        settings_map.put("number_of_shards", 3);
        settings_map.put("number_of_replicas", 1);

        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()//
                .field("dynamic", "true")
                .startObject("properties")
                .startObject("id")
                .field("type", "integer")
                .field("store", "yes")
                .endObject()
                .startObject("name")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .startObject("salary")
                .field("type", "integer")
                .endObject()
                .startObject("team")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("position")
                .field("type", "string")
                .field("index", "not_analyzed")
                .endObject()
                .startObject("description")
                .field("type", "string")
                .field("store", "no")
                .field("index", "analyzed")
                .field("analyzer", "ik_smart")
                .endObject()
                .startObject("addr")
                .field("type", "string")
                .field("store", "yes")
                .field("index", "analyzed")
                .field("analyzer", "ik_smart")
                .endObject()
                .endObject()
                .endObject();

        CreateIndexRequestBuilder prepareCreate = client.admin().indices().prepareCreate("player_info");
        prepareCreate.setSettings(settings_map).addMapping("player", builder).get();

    }
}



