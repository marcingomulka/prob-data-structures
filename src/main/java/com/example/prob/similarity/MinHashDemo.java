package com.example.prob.similarity;


import info.debatty.java.lsh.MinHash;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static com.example.prob.similarity.MinHashDemo.Color.*;
import static org.assertj.core.api.Assertions.assertThat;

public class MinHashDemo {

    public enum Color {
      RED,
      WHITE,
      BLUE,
      YELLOW,
      BLACK,
      GREEN
    };

    @Test
    public void similarity() {
        Set<Color> pattern = Set.of(RED, WHITE, BLUE, YELLOW);
        Set<Color> set1 = Set.of(RED, BLACK, BLUE, YELLOW);
        Set<Color> set2 = Set.of(WHITE, GREEN, YELLOW);

        MinHash minhash = new MinHash(0.1, Color.values().length);

        var patternSignature = minhash.signature(pattern.stream().map(Color::ordinal).collect(Collectors.toSet()));
        var set1Signature = minhash.signature(set1.stream().map(Color::ordinal).collect(Collectors.toSet()));
        var set2Signature = minhash.signature(set2.stream().map(Color::ordinal).collect(Collectors.toSet()));

        System.out.printf("Sig1: %s %n", Arrays.toString(set1Signature));
        System.out.printf("Sig2: %s %n", Arrays.toString(set2Signature));

        double set1Similarity = minhash.similarity(patternSignature, set1Signature);
        double set2Similarity = minhash.similarity(patternSignature, set2Signature);

        System.out.printf("Verifying similarity to set %s %n", pattern);
        System.out.printf("Set 1: %s similarity is %f %n", set1, set1Similarity);
        System.out.printf("Set 2: %s similarity is %f %n", set2, set2Similarity);

        assertThat(set1Similarity).isGreaterThan(set2Similarity);
    }
}
