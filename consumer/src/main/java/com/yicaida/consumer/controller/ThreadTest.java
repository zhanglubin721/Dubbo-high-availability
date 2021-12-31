package com.yicaida.consumer.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author zhanglubin
 * @date 2021/4/20
 */
//测试callbale的能力
public class ThreadTest {

    //线程池，保证项目不会内存溢出
    public static ExecutorService threadPool = Executors.newFixedThreadPool(2);

    static class InsertCallable implements Callable<Boolean>{

        private final List<Object> param;

        public InsertCallable(List<Object> param) {
            this.param = param;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                //insert逻辑
                //param是参数
                List<Object> myParam = this.param;
            } catch (Exception e) {
                //异常处理,sql回退之类的
                return false;
            }
            return true;
        }
    }

    //传参是insert sql的参数
    public Boolean doInsert(List<Object> param) {
        //new线程运行类并传递insert sql参数
        InsertCallable insertCallable = new InsertCallable(param);
        FutureTask<Boolean> insertCallableFutureTask = new FutureTask(insertCallable);
        threadPool.execute(insertCallableFutureTask);
        return true;
    }


    static class WaiMai{}

    static class DianWaiMai implements Callable<Boolean> {
        private WaiMai waimai;

        DianWaiMai(WaiMai waimai){
            this.waimai = waimai;
        }

        @Override
        public Boolean call() throws Exception {
            long waitTime = waimai.hashCode() % 5000;
            Thread.sleep(waitTime);
            return (System.currentTimeMillis() & 1) == 1;
        }
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ExecutorService threadPool = Executors.newFixedThreadPool(2);
        DianWaiMai dianWaiMai = new DianWaiMai(new WaiMai());
        FutureTask<Boolean> futureTask = new FutureTask((Callable) dianWaiMai);

        System.out.println(System.currentTimeMillis() + " 下单外卖，并且去运动");
        threadPool.execute(futureTask);

        Thread.sleep(2000);
        System.out.println(System.currentTimeMillis() + " 运动结束,看看外卖到了没");

        if (!futureTask.isDone()) {
            System.out.println(System.currentTimeMillis() + " 外卖还没到,在等等吧");
            Thread.sleep(1000);
            if (!futureTask.isDone()) {
                System.out.println(System.currentTimeMillis() + " 妈的外卖还没到,取消，吃食堂去");
                futureTask.cancel(false);
            }
        }
        if (!futureTask.isCancelled()) {
            Boolean res = futureTask.get();
            System.out.println(System.currentTimeMillis() + " 外卖到了");
            if (res) {
                System.out.println("太多了吃撑了");
            } else {
                System.out.println("太少了，不够吃，再去食堂吃一点");
            }
        }
        threadPool.shutdown();
    }


}
