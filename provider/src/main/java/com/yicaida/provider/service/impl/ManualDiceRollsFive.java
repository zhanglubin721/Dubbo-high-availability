package com.yicaida.provider.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.IntStream;

//forkJoin无ConcurrentHashMap、带返回值、父线程汇总所有子线程的结果版本
public class ManualDiceRollsFive {
    //投掷两次骰子的次数
    private static final int N = 100000000;
    //一次的占比
    private double fraction = 1.0 / N;
    //每次投2次骰子的点数之和与概率的映射
    private Map<Integer,Double> results;
    private final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);

    private void printResults() {
        //等同于results.entrySet().forEach(entry -> System.out.println(entry));
        results.entrySet().forEach(System.out::println);
    }

    public void simulateDiceRoles() throws InterruptedException {
        ForkJoinTask<Map<Integer, Double>> result = forkJoinPool.submit(makeJob());
        try {
            this.results = result.get();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        //打印结果
        printResults();
        forkJoinPool.shutdown();
//        System.out.println(count.get());
    }

    private CountTask makeJob() {
        CountTask countTask = new CountTask(0,N);
        return countTask;
    }

    private int twoDiceThrows(ThreadLocalRandom random) {
        int firstThrow = random.nextInt(1,7);
        int secondThrow = random.nextInt(1,7);
        return firstThrow + secondThrow;
    }

    private class CountTask extends RecursiveTask<Map<Integer,Double>> {
        private static final int THRESHOLD = 2000000;
        private int start;
        private int end;

        public CountTask(int start,int end) {
            this.start = start;
            this.end = end;
        }
        @Override
        protected Map<Integer,Double> compute() {
            Map<Integer,Double> result = new HashMap<>();
            IntStream.range(2,13).sequential().forEach(i -> result.put(i,0.0));
            ThreadLocalRandom random = ThreadLocalRandom.current();
            boolean canCompute = (end - start) <= THRESHOLD;
            //最终计算,所有的最终拆分都是在这里计算
            if (canCompute) {
                for (int i = start;i < end;i++) {
                    int entry = twoDiceThrows(random);
                    result.compute(entry,(k,v) -> v == 0.0 ? fraction : v + fraction);
//                    accumuLateResult(entry);
//                    count.incrementAndGet();
                }
            }else {
                //并行计算的规模,拆分成50个并行计算
                int step = (start + end) / 50;
                //创建子任务线程集合
                List<CountTask> subTasks = new ArrayList<>();
                //每个并行子任务的开始值
                int pos = start;
                //并行执行50个分叉线程
                for (int i = 0; i < 50; i++) {
                    //每个并行子任务的结束值
                    int lastOne = pos + step;
                    if (lastOne > end) {
                        lastOne = end;
                    }
                    //建立一个子任务的线程
                    CountTask subTask = new CountTask(pos, lastOne);
                    //创建下一个并行子任务的开始值
                    pos += step + 1;
                    //将当前子任务线程添加到线程集合
                    subTasks.add(subTask);
                    //执行该线程,其实是一个递归,判断lastOne-pos是否小于THRESHOLD,小于则真正执行,否则继续分叉50个子线程
                    subTask.fork();
                }
                for (CountTask task : subTasks) {
                    Map<Integer,Double> taskMap = task.join();
                    result.entrySet().stream().forEach(entry -> result.compute(entry.getKey(),
                            (k,v) -> v == 0.0 ? taskMap.get(k) : v + taskMap.get(k)));
                }
            }
            return result;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        ManualDiceRollsFive manualDiceRollsFive = new ManualDiceRollsFive();
        long start = System.currentTimeMillis();
        manualDiceRollsFive.simulateDiceRoles();
        System.out.println(System.currentTimeMillis() - start);
    }
}