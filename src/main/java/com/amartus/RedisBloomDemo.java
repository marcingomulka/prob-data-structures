package com.amartus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisBloomDemo {

    public static final int TOTAL_ELEMENTS = 1000;
    public static final int EXP_INSERTS = TOTAL_ELEMENTS/2;
    public static final double EXP_FFP = 0.1;

    public static final List<String> KEYS = IntStream.range(0, TOTAL_ELEMENTS)
            .boxed()
            .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
            .toList();

    public static final String REDIS_URL = "redis://localhost:6379";
    public static final String BLOOM_1 = "Bloom1";


    public RedissonClient client;

    @BeforeEach
    public void beforeEach() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(REDIS_URL);
        client = Redisson.create();
    }

    @AfterEach
    public void cleanUp() {
        client.getBloomFilter(BLOOM_1).delete();
    }

    @Test
    public void ffpDemo() {
        RBloomFilter<String> bloomFilter = client.getBloomFilter(BLOOM_1);
        bloomFilter.tryInit(EXP_INSERTS, EXP_FFP);

        List<String> present = KEYS.subList(0, EXP_INSERTS);
        List<String> absent = KEYS.subList(EXP_INSERTS, TOTAL_ELEMENTS);

        present.forEach(bloomFilter::add);
        long bloomPositive = present.stream()
                .filter(bloomFilter::contains)
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(bloomFilter::contains)
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
    }


    @Test
    public void overloadDemo() {
        RBloomFilter<String> bloomFilter = client.getBloomFilter(BLOOM_1);
        bloomFilter.tryInit(EXP_INSERTS, EXP_FFP);

        List<String> BIG_KEYS = IntStream.range(0, EXP_INSERTS * 3)
                .boxed()
                .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
                .toList();
        List<String> present = BIG_KEYS.subList(0, BIG_KEYS.size()/2);
        List<String> absent = BIG_KEYS.subList(BIG_KEYS.size()/2, BIG_KEYS.size());

        present.forEach(bloomFilter::add);
        long bloomPositive = present.stream()
                .filter(bloomFilter::contains)
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(bloomFilter::contains)
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
    }
}
