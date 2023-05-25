package com.amartus.prob.membership;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisBloomDemo {

    public static final int TOTAL_ELEMENTS = 1000;
    public static final int EXP_INSERTS = TOTAL_ELEMENTS/2;
    public static final double EXP_FPP = 0.01;

    public static final List<String> KEYS = IntStream.range(0, TOTAL_ELEMENTS)
            .boxed()
            .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
            .toList();

    public static final String REDIS_URL = "redis://localhost:6379";
    public static final String BLOOM_1 = "Bloom1";


    public UnifiedJedis client;

    @BeforeEach
    public void beforeEach() {
        client = new UnifiedJedis(REDIS_URL);
    }

    @AfterEach
    public void cleanUp() {
        client.del(BLOOM_1);
        client.close();
    }

    @Test
    public void fppDemo() {
        client.bfReserve(BLOOM_1, EXP_FPP, EXP_INSERTS);
        System.out.println(String.format("BF info: %s", client.bfInfo(BLOOM_1)));


        List<String> present = KEYS.subList(0, EXP_INSERTS);
        List<String> absent = KEYS.subList(EXP_INSERTS, TOTAL_ELEMENTS);

        present.forEach(item -> client.bfAdd(BLOOM_1, item));
        long bloomPositive = present.stream()
                .filter(item -> client.bfExists(BLOOM_1, item))
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(item -> client.bfExists(BLOOM_1, item))
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
    }


    @Test
    public void overfillDemo() {
        client.bfReserve(BLOOM_1, EXP_FPP, EXP_INSERTS);
        System.out.println(String.format("BF info: %s", client.bfInfo(BLOOM_1)));

        List<String> BIG_KEYS = IntStream.range(0, EXP_INSERTS * 3)
                .boxed()
                .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
                .toList();
        List<String> present = BIG_KEYS.subList(0, BIG_KEYS.size()/2);
        List<String> absent = BIG_KEYS.subList(BIG_KEYS.size()/2, BIG_KEYS.size());

        present.forEach(item -> client.bfAdd(BLOOM_1, item));
        long bloomPositive = present.stream()
                .filter(item -> client.bfExists(BLOOM_1, item))
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(item -> client.bfExists(BLOOM_1, item))
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
        System.out.println(String.format("BF info: %s", client.bfInfo(BLOOM_1)));

        //Redis allocates more subfiters when overfill occurs. This however slightly degrades access time - because those subfilters are one after another.
    }
}
