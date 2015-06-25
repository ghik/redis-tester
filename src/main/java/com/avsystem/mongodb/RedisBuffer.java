package com.avsystem.mongodb;

import com.google.common.base.Throwables;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.joda.time.DateTime;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class RedisBuffer {
    private Jedis jedis = new Jedis();
    private Jedis rwJedis = new Jedis();

    private MongoCollection<Document> collection = new MongoClient().getDatabase("test").getCollection("samples");

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private void recordSample(DateTime tstamp, double value) {
        DateTime secondOffset = tstamp.minusMillis(tstamp.getMillisOfSecond());

        Transaction t = jedis.multi();
        t.incrByFloat("sample:" + secondOffset, value);
        t.sadd("seconds", String.valueOf(secondOffset.getMillis()));
        t.exec();
    }

    private void rewriteToMongo() {
        List<DateTime> secondsInRedis = rwJedis.smembers("seconds").stream()
                .map(s -> new DateTime(Long.parseLong(s))).sorted().collect(Collectors.toList());

        if (secondsInRedis.size() <= 1) {
            return;
        }

        for (DateTime secondToRewrite : secondsInRedis.subList(0, secondsInRedis.size() - 1)) {
            double value = Double.parseDouble(rwJedis.get("sample:" + secondToRewrite));
            collection.insertOne(
                    new Document("second", secondToRewrite.toDate())
                            .append("value", value));

            Transaction t = rwJedis.multi();
            t.srem("seconds", String.valueOf(secondToRewrite.getMillis()));
            t.del("sample:" + secondToRewrite);
            t.exec();
        }
    }

    private void test() {
        executor.scheduleAtFixedRate(this::rewriteToMongo, 5, 5, TimeUnit.SECONDS);

        while (true) {
            double sample = ThreadLocalRandom.current().nextDouble();
            recordSample(new DateTime(), sample);

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static void main(String[] args) {
        new RedisBuffer().test();
    }
}
