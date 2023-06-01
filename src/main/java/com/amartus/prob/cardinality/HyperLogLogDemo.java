package com.amartus.prob.cardinality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.UnifiedJedis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static java.lang.Math.abs;
import static java.lang.Math.max;

public class HyperLogLogDemo {

    public static final String REDIS_URL = "redis://localhost:6379";
    public static final String PAN_TADEUSZ_WORDS = "PanTadeuszWordsSet";
    public static final String KRZYZACY_WORDS = "KrzyzacyWordsSet";

    public UnifiedJedis client;

    @BeforeEach
    public void beforeEach() {
        client = new UnifiedJedis(REDIS_URL);
    }

    @AfterEach
    public void cleanUp() {
        client.del(PAN_TADEUSZ_WORDS);
        client.del(KRZYZACY_WORDS);
        client.close();
    }


    private void forEachWordInFile(String fileName, Consumer<String> consumer) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            reader.lines().forEach(line ->{
                for (String word : line.split("\\s+")) {
                    word = word.replaceAll("[,)(.\"\\-!?]", "")
                            .toLowerCase();
                    consumer.accept(word);
                }
            });
        }
    }

    @Test
    public void countWords() throws IOException {

        Set<String> uniqueWords = new HashSet<>();
        forEachWordInFile("PanTadeusz.txt", word -> {
            //standard set of words
            uniqueWords.add(word);
            //hyperloglog
            client.pfadd(PAN_TADEUSZ_WORDS, word);
        });

        long hyperCount = client.pfcount(PAN_TADEUSZ_WORDS);
        long setCount = uniqueWords.size();
        System.out.println(String.format("Total approx words: %d", hyperCount));
        System.out.println(String.format("Total words: %d, error: %f", setCount, abs(setCount - hyperCount)/max(1.0, setCount)));
    }



    @Test
    public void mergeSets() throws IOException {
        Set<String> uniqueWords1 = new HashSet<>();

        forEachWordInFile("PanTadeusz.txt", word -> {
            uniqueWords1.add(word);
            client.pfadd(PAN_TADEUSZ_WORDS, word);
        });
        System.out.println(String.format("%s: Total approx words: %d", PAN_TADEUSZ_WORDS, client.pfcount(PAN_TADEUSZ_WORDS)));
        System.out.println(String.format("Total words: %d", uniqueWords1.size()));

        Set<String> uniqueWords2 = new HashSet<>();
        forEachWordInFile("KrzyzacyT1.txt", word -> {
            uniqueWords2.add(word);
            client.pfadd(KRZYZACY_WORDS, word);
        });
        System.out.println(String.format("%s: Total approx words: %d", KRZYZACY_WORDS, client.pfcount(KRZYZACY_WORDS)));
        System.out.println(String.format("Total words: %d", uniqueWords2.size()));

        client.pfmerge(KRZYZACY_WORDS, PAN_TADEUSZ_WORDS);
        uniqueWords1.addAll(uniqueWords2);

        long hyperCount = client.pfcount(KRZYZACY_WORDS);
        long setCount = uniqueWords1.size();
        System.out.println(String.format("Merged sets approx words: %d", hyperCount));
        System.out.println(String.format("Merged sets total words: %d, error: %f", setCount, abs(setCount - hyperCount)/max(1.0, setCount)));
    }
}
