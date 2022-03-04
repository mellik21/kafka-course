package com.github.mellik21.util;

public class ConstantKeeper {

    private ConstantKeeper() {
    }


    public static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    public static final String TOPIC_NAME = "example_topic";
    public static final String GROUP_ID = "kafka-course";
    public static final int HUNDRED_MILLISECOND = 100;
}
