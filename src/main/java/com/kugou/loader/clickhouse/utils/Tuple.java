package com.kugou.loader.clickhouse.utils;

import java.io.Serializable;

/**
 * Created by jaykelin on 2017/4/5.
 */
public abstract class Tuple implements Serializable {

    private static final long serialVersionUID = -8719929625763890308L;

    public static <A, B> Tuple2<A, B> tuple(A a, B b) {
        return new Tuple2<>(a, b);
    }

    public static class Tuple2<A, B> extends Tuple implements Serializable{

        private static final long serialVersionUID = 7263645006696591635L;

        private A a;
        private B b;

        private Tuple2(A a, B b) {
            this.a = a;
            this.b = b;
        }

        public A _1() {
            return this.a;
        }

        public B _2() {
            return this.b;
        }
    }
}
