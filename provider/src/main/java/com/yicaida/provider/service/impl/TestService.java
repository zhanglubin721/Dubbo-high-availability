package com.yicaida.provider.service.impl;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author zhanglubin
 * @date 2021/6/15
 */
public class TestService {

    //冒泡排序 数组实现
    @Test
    public void test1() {
        int[] datas = initDataList();
        int temporary;
        for (int i = 0; i < datas.length; i++) {
            for (int j = 1; j < datas.length - i; j++) {
                if (datas[j - 1] > datas[j]) {
                    temporary = datas[j];
                    datas[j] = datas[j - 1];
                    datas[j - 1] = temporary;
                }
            }
        }
        for (int data : datas) {
            System.out.println(data);
        }
    }
    //冒泡排序 ArrayList实现
    @Test
    public void test11() {
        int temporary;
        ArrayList<Integer> datas = initDataArrayList();
        for (int i = 0; i < datas.size(); i++) {
            for (int j = 1; j < datas.size() - i; j++) {
                if (datas.get(j) < datas.get(j - 1)) {
                    temporary = datas.get(j);
                    datas.set(j, datas.get(j - 1));
                    datas.set(j - 1, temporary);
                }
            }
        }
        for (Integer data : datas) {
            System.out.println(data);
        }
    }

    //选择排序 数组实现
    @Test
    public void test2() {
        int temporaryIndex;
        int temporary;
        int[] datas = initDataList();
        for (int i = 0; i < datas.length; i++) {
            temporaryIndex = i;
            for (int j = i + 1; j < datas.length; j++) {
                if (datas[j] < datas[temporaryIndex]) {
                    temporaryIndex = j;
                }
            }
            if (temporaryIndex != i) {
                temporary = datas[i];
                datas[i] = datas[temporaryIndex];
                datas[temporaryIndex] = temporary;
            }
        }
        for (int data : datas) {
            System.out.println(data);
        }
    }

    //选择排序 ArrayList实现
    @Test
    public void test22() {
        int temporaryIndex;
        int temporary;
        ArrayList<Integer> datas = initDataArrayList();
        for (int i = 0; i < datas.size(); i++) {
            temporaryIndex = i;
            for (int j = i + 1; j < datas.size(); j++) {
                if (datas.get(j) < datas.get(temporaryIndex)) {
                    temporaryIndex = j;
                }
            }
            if (temporaryIndex != i) {
                temporary = datas.get(i);
                datas.set(i, datas.get(temporaryIndex));
                datas.set(temporaryIndex, temporary);
            }
        }
        for (Integer data : datas) {
            System.out.println(data);
        }
    }

    //插入排序 数组实现
    @Test
    public void test3() {
        int temporary;
        int[] datas = initDataList();
        for (int i = 0; i < datas.length; i++) {
            for (int j = i + 1; j >= 1 && j < datas.length; j--) {
                if (datas[j] < datas[j - 1]) {
                    temporary = datas[j];
                    datas[j] = datas[j - 1];
                    datas[j - 1] = temporary;
                }
            }
        }
        for (int data : datas) {
            System.out.println(data);
        }
    }

    //插入排序 ArrayList实现
    @Test
    public void test33() {
        int temporary;
        ArrayList<Integer> datas = initDataArrayList();
        for (int i = 0; i < datas.size(); i++) {
            for (int j = i + 1; j >= 1 && j < datas.size(); j--) {
                if (datas.get(j) < datas.get(j - 1)) {
                    temporary = datas.get(j);
                    datas.set(j, datas.get(j - 1));
                    datas.set(j - 1, temporary);
                }
            }
        }
        for (Integer data : datas) {
            System.out.println(data);
        }
    }

    @Test
    public void test4() {
        int[] datas = initDataList();
        int[] ints = ShellSort2(datas);
        for (int anInt : ints) {
            System.out.println(anInt);
        }
    }

    private static int[] ShellSort2(int[] arr) {
        //step:步长
        for (int step = arr.length / 2; step > 0; step /= 2) {
            //对一个步长区间进行比较 [step,arr.length)
            for (int i = step; i < arr.length; i++) {
                int value = arr[i];
                int j;
                //对步长区间中具体的元素进行比较
                for (j = i - step; j >= 0 && arr[j] > value; j -= step) {
                    //j为左区间的取值，j+step为右区间与左区间的对应值。
                    arr[j + step] = arr[j];
                }
                //此时step为一个负数，[j + step]为左区间上的初始交换值
                arr[j + step] = value;
            }
        }
        return arr;
    }

    public static int[] ShellSort(int[] array) {
        int len = array.length;
        int temp, gap = len / 2;
        while (gap > 0) {
            for (int i = gap; i < len; i++) {
                temp = array[i];
                int preIndex = i - gap;
                while (preIndex >= 0 && array[preIndex] > temp) {
                    array[preIndex + gap] = array[preIndex];
                    preIndex -= gap;
                }
                array[preIndex + gap] = temp;
            }
            gap /= 2;
        }
        return array;
    }



    public int[] initDataList() {
        int[] datas = new int[20];
        datas[0] = 42;
        datas[1] = 97;
        datas[2] = 86;
        datas[3] = 53;
        datas[4] = 75;
        datas[5] = 89;
        datas[6] = 31;
        datas[7] = 21;
        datas[8] = 6;
        datas[9] = 56;
        datas[10] = 76;
        datas[11] = 4598;
        datas[12] = 524;
        datas[13] = 645;
        datas[14] = 87;
        datas[15] = 689;
        datas[16] = 764;
        datas[17] = 5;
        datas[18] = 2345;
        datas[19] = 43;
        return datas;
    }

    public ArrayList<Integer> initDataArrayList() {
        ArrayList<Integer> datas = new ArrayList<Integer>();
        datas.add(42);
        datas.add(97);
        datas.add(86);
        datas.add(53);
        datas.add(75);
        datas.add(89);
        datas.add(31);
        datas.add(21);
        datas.add(6);
        datas.add(56);
        datas.add(76);
        datas.add(4598);
        datas.add(524);
        datas.add(645);
        datas.add(87);
        datas.add(689);
        datas.add(764);
        datas.add(5);
        datas.add(2345);
        datas.add(43);
        return datas;
    }

}
