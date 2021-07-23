package com.yicaida.consumer.controller;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zhanglubin
 * @date 2021/7/23
 */
public class conditionTest {

    /**
     * 线程操作资源类
     */
    /**
     * 1代表A线程
     * 2代表B线程
     * 3代表C线程
     */
    private volatile int number = 1;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition c1 = lock.newCondition();
    private final Condition c2 = lock.newCondition();
    private final Condition c3 = lock.newCondition();

    public void print5() {
        lock.lock();
        try {
            // 判断
            while (number != 1) {
//                System.out.println("await1...");
                // 等待
                c1.await();
//                System.out.println("await2...");
            }
            // 干活
            for (int i = 1; i <= 5; i++) {
                System.out.println(Thread.currentThread().getName()+"\t"+i);
            }
            // 通知
            number = 2;// 修改标志位
            c2.signal();// 通知B
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void print10() {
        lock.lock();
        try {
            // 判断
            while (number != 2) {
                // 等待
                c2.await();
            }
            // 干活
            for (int i = 1; i <= 10; i++) {
                System.out.println(Thread.currentThread().getName()+"\t"+i);
            }
            // 通知
            number = 3;// 修改标志位
            c3.signal();// 通知C
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public void print15() {
        lock.lock();
        try {
            // 判断
            while (number != 3) {
                // 等待
                c3.await();
            }
            // 干活
            for (int i = 1; i <= 15; i++) {
                System.out.println(Thread.currentThread().getName()+"\t"+i);
            }
            // 通知
            number = 1;// 修改标志位
            c1.signal();// 通知A
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
}
