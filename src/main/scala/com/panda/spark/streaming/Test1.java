package com.panda.spark.streaming;

import java.io.Serializable;

public class Test1 implements Serializable {

    public Integer i = 0;

    public Integer add() {
        return ++i;
    }
}
