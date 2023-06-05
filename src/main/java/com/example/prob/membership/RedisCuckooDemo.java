package com.example.prob.membership;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class RedisCuckooDemo {

    public static final int TOTAL_ELEMENTS = 1000;
    public static final int EXP_INSERTS = TOTAL_ELEMENTS/2;

    public static final List<String> KEYS = IntStream.range(0, TOTAL_ELEMENTS)
            .boxed()
            .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
            .toList();

    public static final String REDIS_URL = "redis://localhost:6379";
    public static final String CUCKOO_1 = "Cuckoo1";

    public UnifiedJedis client;

    @BeforeEach
    public void beforeEach() {
        client = new UnifiedJedis(REDIS_URL);
    }

    @AfterEach
    public void cleanUp() {
        client.del(CUCKOO_1);
        client.close();
    }

    @Test
    public void fppDemo() {
        client.cfReserve(CUCKOO_1, EXP_INSERTS);

        List<String> present = KEYS.subList(0, EXP_INSERTS);
        List<String> absent = KEYS.subList(EXP_INSERTS, TOTAL_ELEMENTS);

        present.forEach(item -> client.cfAdd(CUCKOO_1, item));
        long bloomPositive = present.stream()
                .filter(item -> client.cfExists(CUCKOO_1, item))
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long cuckooFalsePositive = absent.stream()
                .filter(item -> client.cfExists(CUCKOO_1, item))
                .count();
        System.out.println(String.format("Cuckoo filter false positives found: %d, ratio:%f", cuckooFalsePositive, (double)cuckooFalsePositive/(double)TOTAL_ELEMENTS));
        System.out.println(String.format("CF info: %s", client.cfInfo(CUCKOO_1)));
    }

    @Test
    public void delDemo() {
        client.cfReserve(CUCKOO_1, EXP_INSERTS);
        System.out.println(String.format("CF info: %s", client.cfInfo(CUCKOO_1)));


        List<String> present = KEYS.subList(0, EXP_INSERTS);

        present.forEach(item -> client.cfAdd(CUCKOO_1, item));
        long bloomPositive = present.stream()
                .filter(item -> client.cfExists(CUCKOO_1, item))
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        client.cfDel(CUCKOO_1, present.get(0));
        client.cfDel(CUCKOO_1, present.get(1));
        client.cfDel(CUCKOO_1, present.get(2));

        assertThat(client.cfExists(CUCKOO_1, present.get(0))).isFalse();
        assertThat(client.cfExists(CUCKOO_1, present.get(1))).isFalse();
        assertThat(client.cfExists(CUCKOO_1, present.get(2))).isFalse();

        System.out.println(String.format("CF info: %s", client.cfInfo(CUCKOO_1)));
    }


    @Test
    public void overfillDemo() {
        client.cfReserve(CUCKOO_1, EXP_INSERTS);
        System.out.println(String.format("CF info: %s", client.cfInfo(CUCKOO_1)));

        List<String> BIG_KEYS = IntStream.range(0, EXP_INSERTS * 3)
                .boxed()
                .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
                .toList();
        List<String> present = BIG_KEYS.subList(0, BIG_KEYS.size()/2);
        List<String> absent = BIG_KEYS.subList(BIG_KEYS.size()/2, BIG_KEYS.size());

        present.forEach(item -> client.cfAdd(CUCKOO_1, item));
        long cuckooPositive = present.stream()
                .filter(item -> client.cfExists(CUCKOO_1, item))
                .count();

        assertThat(cuckooPositive).isEqualTo(present.size());

        long cuckooFalsePositive = absent.stream()
                .filter(item -> client.cfExists(CUCKOO_1, item))
                .count();
        System.out.println(String.format("Cuckoo filter false positives found: %d, ratio:%f", cuckooFalsePositive, (double)cuckooFalsePositive/(double)(EXP_INSERTS * 3)));
        System.out.println(String.format("CF info: %s", client.cfInfo(CUCKOO_1)));


    }

}
