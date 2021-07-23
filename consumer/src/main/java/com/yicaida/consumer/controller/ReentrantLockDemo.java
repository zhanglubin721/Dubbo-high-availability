package com.yicaida.consumer.controller;

/**
 * @author zhanglubin
 * @date 2021/7/23
 */
public class ReentrantLockDemo {

    public static void main(String[] args) {
        conditionTest sharedResource = new conditionTest();
        int num = 5;

        for (int i = 1; i <= num; i++) {
            new Thread(() -> {
                sharedResource.print10();
            }, "B").start();
        }
        for (int i = 1; i <= num; i++) {
            new Thread(() -> {
                sharedResource.print15();
            }, "C").start();
        }
        for (int i = 1; i <= num; i++) {
            new Thread(() -> {
                sharedResource.print5();
            }, "A").start();
        }
    }
}
