package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongodbConnTest {
    public static void main(String[] args) {
        MongoClient mongoClient = getMongoClient();
        MongoDatabase dms_admin1 = mongoClient.getDatabase("dms_admin");
        dms_admin1.getCollection("").find(Document.parse("")).limit(10);
        String cmd  = "db.getCollection(\"2357\").find({}).limit(10)";
        String json = "{ count: \"notice\", query: { companyId: \"735\", receiver: \"6921\", status: \"unread\", isDeleted: \"0\" } }";
        Document collStatsResults = dms_admin1.runCommand(Document.parse(json));
        System.out.println(JSON.toJSONString(collStatsResults));
//        long dms_admin = dms_admin1.getCollection("2354").count();

//        System.out.println(dms_admin);
    }
    public static MongoClient getMongoClient() {
        String[] hosts = StringUtils.split("172.31.88.242,172.31.88.241", ",");
        //TODO 临时使用用户密码,后续迁移到标准BO的服务中
        String database = "admin";
        String username = "admin";
        String password = "******";
        final Pattern hostAndPort = Pattern.compile("^(.[^:]*){1}([:]){0,1}(\\d+){0,1}$");
        List<ServerAddress> serverAddresses = Lists.transform(Arrays.asList(hosts), new Function<String, ServerAddress>() {
            @Override
            public ServerAddress apply(String string) {
                Matcher matcher = hostAndPort.matcher(string.trim());
                if (matcher.matches()) {
                    String hostname = matcher.group(1);
                    String port = matcher.group(3);
                    return new ServerAddress(hostname, port != null ? Integer.parseInt(port) : 27017);

                } else {
                    throw new IllegalArgumentException(string + " doesn't appear to be a hostname.");
                }
            }
        });
        if (username != null && password != null) {
            return new MongoClient(serverAddresses, Arrays.asList(MongoCredential.createCredential(username, database, password.toCharArray())));
        } else {
            return new MongoClient(serverAddresses);
        }
    }
}


