package com.yicaida.provider.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

//单线程版本
public class ManualDiceRollsThree {
    //投掷两次骰子的次数
    private static final int N = 100000000;
    //一次的占比
    private double fraction = 1.0 / N;
    //每次投2次骰子的点数之和与概率的映射
    private Map<Integer,Double> results = new HashMap<>();

    private void printResults() {
        //等同于results.entrySet().forEach(entry -> System.out.println(entry));
        results.entrySet().forEach(System.out::println);
    }

    public void simulateDiceRoles() throws InterruptedException {
        for (int i = 0;i < N;i++) {
            int entry = twoDiceThrows();
            results.compute(entry,(k,v) -> v == null ? fraction : v + fraction);
        }
        printResults();
    }

    private int twoDiceThrows() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int firstThrow = random.nextInt(1,7);
        int secondThrow = random.nextInt(1,7);
        return firstThrow + secondThrow;
    }

    public static void main(String[] args) throws InterruptedException {
        ManualDiceRollsThree manualDiceRollsThree = new ManualDiceRollsThree();
        long start = System.currentTimeMillis();
        manualDiceRollsThree.simulateDiceRoles();
        System.out.println(System.currentTimeMillis() - start);
    }
}
