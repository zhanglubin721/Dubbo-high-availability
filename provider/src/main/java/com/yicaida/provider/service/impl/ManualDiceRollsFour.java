package com.yicaida.provider.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

//forkJoin + ConcurrentHashMap版本
public class ManualDiceRollsFour {
    //投掷两次骰子的次数
    private static final int N = 100000000;
    //一次的占比
    private double fraction = 1.0 / N;
    //每次投2次骰子的点数之和与概率的映射
    private Map<Integer,Double> results = new ConcurrentHashMap<>();
    private final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() * 2);

    private AtomicInteger count = new AtomicInteger(0);

    private void printResults() {
        //等同于results.entrySet().forEach(entry -> System.out.println(entry));
        results.entrySet().forEach(System.out::println);
    }

    public void simulateDiceRoles() throws InterruptedException {
        ForkJoinTask<Void> task = forkJoinPool.submit(makeJob());
        task.join();
        //打印结果
        printResults();
//        System.out.println(count.get());
    }

    private CountTask makeJob() {
        CountTask countTask = new CountTask(0,N);
        return countTask;
    }

    private void accumuLateResult(int entry) {
        //Map的compute方法第二参数为BiFunction的函数式接口，给定两种不同的参数对象，返回另一个结果对象，这三种对象
        //可以相同，可以不同
        //如果results的entry键的值为null(该键不存在)，则把该值设为fraction(单次概率亿分之一)
        //否则将该键的值设为原值加上fraction(单次概率亿分之一)
        results.compute(entry,(key,previous) -> previous == null ? fraction : previous + fraction);
    }

    private int twoDiceThrows() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int firstThrow = random.nextInt(1,7);
        int secondThrow = random.nextInt(1,7);
        return firstThrow + secondThrow;
    }

    private class CountTask extends RecursiveAction {
        private static final int THRESHOLD = 2000000;
        private int start;
        private int end;

        public CountTask(int start,int end) {
            this.start = start;
            this.end = end;
        }
        @Override
        protected void compute() {
            boolean canCompute = (end - start) <= THRESHOLD;
            //最终计算,所有的最终拆分都是在这里计算
            if (canCompute) {
                for (int i = start;i < end;i++) {
                    int entry = twoDiceThrows();
                    accumuLateResult(entry);
//                    count.incrementAndGet();
                }
            } else {
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
                    task.join();
                }
            }
        }
    }
    //我的理解：
    //结果是会有一个专门生产子线程的父线程，生产出来的子线程去执行真正的任务
    //所以父线程生成子线程与子线程内部进行真正的计算是同步完成的

    public static void main(String[] args) throws InterruptedException {
        ManualDiceRollsFour manualDiceRollsFour = new ManualDiceRollsFour();
        long start = System.currentTimeMillis();
        manualDiceRollsFour.simulateDiceRoles();
        System.out.println(System.currentTimeMillis() - start);
    }
}
