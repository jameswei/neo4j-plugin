package com.chitu.neo4j.plugin;

import com.chitu.neo4j.serialization.msgpack.MPResult;
import com.chitu.neo4j.serialization.protobuf.PBResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.neo4j.graphdb.*;
import org.neo4j.server.plugins.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jwei on 11/23/15.
 */
@Description("get all directors")
public class Director extends ServerPlugin {
    private static final Label DIRECTOR = DynamicLabel.label("Director");
    private static final String PROP_KEY_ID = "id";
    private static final String PROP_KEY_NAME = "name";
    private static final String CHARSET_UTF8 = "utf-8";

    @Name("protobuf")
    @PluginTarget(GraphDatabaseService.class)
    public String getDirectorsInProtobuf(@Source GraphDatabaseService graphDB) throws IOException {
        try (Transaction tx = graphDB.beginTx()) {
            ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
            PBResult.Result.Builder resultBuilder = PBResult.Result.newBuilder();
            PBResult.Item.Builder itemBuilder = PBResult.Item.newBuilder();
            Node director;
            while (directors.hasNext()) {
                director = directors.next();
                resultBuilder.addItems(itemBuilder.setId(director.getId())
                        .setName(director.getProperty(PROP_KEY_NAME).toString())
                        .build());
                //itemBuilder.clear();
            }
            tx.success();
            byte[] serialized = resultBuilder.build().toByteArray();
            System.out.println("total length in Protobuf: " + serialized.length);
            return new String(serialized, CHARSET_UTF8);
        }
    }

    @Name("json")
    @PluginTarget(GraphDatabaseService.class)
    public String getDirectorsInJson(@Source GraphDatabaseService graphDB) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (Transaction tx = graphDB.beginTx()) {
            ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
            Node director;
            List<Map<String, Object>> result = new LinkedList<>();
            Map<String, Object> item;
            while (directors.hasNext()) {
                director = directors.next();
                item = new HashMap<>();
                item.put(PROP_KEY_ID, director.getId());
                item.put(PROP_KEY_NAME, director.getProperty(PROP_KEY_NAME).toString());
                result.add(item);
            }
            tx.success();
            String jsonized = objectMapper.writeValueAsString(result);
            System.out.println("total length in JSON: " + jsonized.length());
            return jsonized;
        }
    }

    @Name("msgpack")
    @PluginTarget(GraphDatabaseService.class)
    public String getDirectorsInMsgPack(@Source GraphDatabaseService graphDB) throws IOException {
        MessagePack msgpack = new MessagePack();
        msgpack.register(MPResult.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Packer packer = msgpack.createPacker(out);
        try (Transaction tx = graphDB.beginTx()) {
            ResourceIterator<Node> directors = graphDB.findNodes(DIRECTOR);
            Node director;
            while (directors.hasNext()) {
                director = directors.next();
                MPResult item = new MPResult();
                item.id = director.getId();
                item.name = director.getProperty(PROP_KEY_NAME).toString();
                packer.write(item);
            }
            tx.success();
            byte[] serialized = out.toByteArray();
            System.out.println("total length in MsgPack: " + serialized.length);
            return new String(serialized, CHARSET_UTF8);
        }
    }
}
