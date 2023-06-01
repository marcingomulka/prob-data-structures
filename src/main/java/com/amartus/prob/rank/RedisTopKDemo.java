package com.amartus.prob.rank;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

import java.util.Map;
import java.util.stream.IntStream;

public class RedisTopKDemo {


    public static final int TOTAL_ELEMENTS = 100;

    public static final String REDIS_URL = "redis://localhost:6379";
    public static final String HEAVY_1 = "Heavy1";

    public UnifiedJedis client;

    @BeforeEach
    public void beforeEach() {
        client = new UnifiedJedis(REDIS_URL);
    }

    @AfterEach
    public void cleanUp() {
        client.del(HEAVY_1);
        client.close();
    }

    @Test
    public void topkRank() {
        client.topkReserve(HEAVY_1, 5);
        System.out.println(String.format("TopK info: %s", client.topkInfo(HEAVY_1)));
        System.out.println(String.format("Init: TopK list: %s", client.topkList(HEAVY_1)));

        IntStream.range(0, TOTAL_ELEMENTS).forEach(i -> client.topkIncrBy(HEAVY_1, Map.of(String.format("Item_%d", i), 5L)));
        IntStream.range(0, TOTAL_ELEMENTS/2).forEach(i -> client.topkIncrBy(HEAVY_1, Map.of(String.format("Item_%d", i), 5L)));
        IntStream.range(0, TOTAL_ELEMENTS/4).forEach(i -> client.topkIncrBy(HEAVY_1, Map.of(String.format("Item_%d", i), 5L)));

        IntStream.of(13, 20, 15).forEach(i -> client.topkIncrBy(HEAVY_1, Map.of(String.format("Item_%d", i), 10L)));
        client.topkIncrBy(HEAVY_1, Map.of("Item_13", 5L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_20", 10L));

        System.out.println(String.format("Final: TopK list: %s", client.topkList(HEAVY_1)));

    }


    @Test
    public void incorrectRank() {
        client.topkReserve(HEAVY_1, 5);
        System.out.println(String.format("TopK info: %s", client.topkInfo(HEAVY_1)));
        System.out.println(String.format("Init: TopK list: %s", client.topkList(HEAVY_1)));

        //increment all items by 5
        IntStream.range(0, TOTAL_ELEMENTS).forEach(i -> client.topkIncrBy(HEAVY_1, Map.of(String.format("Item_%d", i), 20L)));

        //Step 1: increment Items no 3, 21, 33, 40, 50, 60, 78 by 1
        client.topkIncrBy(HEAVY_1, Map.of("Item_3", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_21", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_33", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_40", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_50", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_60", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_78", 1L));

        System.out.println(String.format("Step 1: TopK list: %s", client.topkList(HEAVY_1)));

        //Step 2: increment Items no 3, 21, 60 by 1
        client.topkIncrBy(HEAVY_1, Map.of("Item_3", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_21", 1L));
        client.topkIncrBy(HEAVY_1, Map.of("Item_60", 1L));

        System.out.println(String.format("Step 2: TopK list: %s", client.topkList(HEAVY_1)));

        //Step 3: increment Items no 78 by 5
        client.topkIncrBy(HEAVY_1, Map.of("Item_78", 5L));

        System.out.println(String.format("Step 3: TopK list: %s", client.topkList(HEAVY_1)));
        System.out.println(String.format("TopK info: %s", client.topkInfo(HEAVY_1)));

    }
}
