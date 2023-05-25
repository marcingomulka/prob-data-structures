package com.amartus.prob.membership;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class GuavaBloomDemo {

    public static final int TOTAL_ELEMENTS = 1000;
    public static final int EXP_INSERTS = TOTAL_ELEMENTS/2;
    public static final double EXP_FPP = 0.01;

    public static final List<String> KEYS = IntStream.range(0, TOTAL_ELEMENTS)
            .boxed()
            .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
            .toList();

    @Test
    public void fppDemo() {
        BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), EXP_INSERTS, EXP_FPP);

        List<String> present = KEYS.subList(0, EXP_INSERTS);
        List<String> absent = KEYS.subList(EXP_INSERTS, TOTAL_ELEMENTS);

        present.forEach(bloomFilter::put);
        long bloomPositive = present.stream()
                .filter(bloomFilter)
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(bloomFilter)
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
    }

    @Test
    public void mergeDemo() {
        BloomFilter<String> bloomFilter1 = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), EXP_INSERTS, EXP_FPP);
        BloomFilter<String> bloomFilter2 = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), EXP_INSERTS, EXP_FPP);

        List<String> first = KEYS.subList(0, 50);
        List<String> second = KEYS.subList(50, 100);

        first.forEach(bloomFilter1::put);
        second.forEach(bloomFilter2::put);


        if (!bloomFilter1.isCompatible(bloomFilter2)) {
            throw new IllegalStateException("Filters not compatible");
        }
        bloomFilter1.putAll(bloomFilter2);

        long firstHitCount = first.stream()
                .filter(bloomFilter1)
                .count();
        long secondHitCount = second.stream()
                .filter(bloomFilter1)
                .count();

        assertThat(firstHitCount + secondHitCount).isEqualTo(first.size() + second.size());
    }

    @Test
    public void overfillDemo() {
        BloomFilter<String> bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), EXP_INSERTS, EXP_FPP);

        List<String> BIG_KEYS = IntStream.range(0, EXP_INSERTS * 3)
                .boxed()
                .map(i -> String.format("%d:%s", i, UUID.randomUUID()))
                .toList();

        List<String> present = BIG_KEYS.subList(0, BIG_KEYS.size()/2);
        List<String> absent = BIG_KEYS.subList(BIG_KEYS.size()/2, BIG_KEYS.size());

        present.forEach(bloomFilter::put);
        long bloomPositive = present.stream()
                .filter(bloomFilter)
                .count();

        assertThat(bloomPositive).isEqualTo(present.size());

        long bloomFalsePositive = absent.stream()
                .filter(bloomFilter)
                .count();
        System.out.println(String.format("Bloom filter false positives found: %d, ratio:%f", bloomFalsePositive, (double)bloomFalsePositive/(double)TOTAL_ELEMENTS));
        System.out.println(String.format("Bloom filter predicted fpp: %f", bloomFilter.expectedFpp()));

        //Overfilling a guava bloom filter degrades its expected False Positive probability.
    }
}
