package com.kugou.test;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.Test;

import java.util.UUID;

/**
 * Created by jaykelin on 2017/7/26.
 */

public class ShardingTest {

    private HashFunction hashFn = Hashing.murmur3_128();

    @Test
    public void shardTest(){
        int code = hashFn.hashString("20170107").asInt() & Integer.MAX_VALUE;
        int index = code % 3;
        System.out.println(index);
    }
}
