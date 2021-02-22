package com.yicaida.provider;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ZlbTest {

    public ArrayList<Integer> getData() {
        ArrayList<Integer> data = new ArrayList<Integer>();
        data.add(42);
        data.add(97);
        data.add(86);
        data.add(53);
        data.add(75);
//        data.add(89);
//        data.add(31);
//        data.add(21);
//        data.add(6);
//        data.add(56);
//        data.add(76);
//        data.add(4598);
//        data.add(524);
//        data.add(645);
//        data.add(87);
//        data.add(689);
//        data.add(764);
//        data.add(5);
//        data.add(2345);
        return data;
    }

    //冒泡排序
    @Test
    public void test1() {
        ArrayList<Integer> data = getData();

        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < data.size() - 1 - i; j++) {
                if(data.get(j) > data.get(j + 1)) {
                    Integer num = data.get(j);
                    data.remove(j);
                    data.add(j, data.get(j));
                    data.remove(j + 1);
                    data.add(j + 1, num);
                }
            }
        }
        for (Integer datum : data) {
            System.out.println(datum);
        }
    }

    //选择排序
    @Test
    public void test2() {
        ArrayList<Integer> data = getData();

        for (int i = 0; i < data.size(); i++) {
            Integer min = data.get(i);
            int index = 1;
            for (int j = i + 1; j < data.size(); j++) {
                if(data.get(j) < min) {
                    min = data.get(j);
                    index = j;
                }
            }
            if(min != data.get(i)) {
                Integer num = data.get(i);
                data.remove(i);
                data.add(i, min);
                data.remove(index);
                data.add(index, num);
            }
        }

        for (Integer datum : data) {
            System.out.println(datum);
        }
    }

    //冒泡排序
    @Test
    public void test11() {
        ArrayList<Integer> data = getData();
        if(data.size() <= 2) {
            return;
        }

        for (int i = 0; i < data.size(); i++) {
            for (int j = 0; j < data.size() - 1 - i; j++) {
                if(data.get(j) > data.get(j + 1)) {
                    Integer num = data.get(j);
                    data.remove(j);
                    data.add(j, data.get(j));
                    data.remove(j + 1);
                    data.add(j + 1, num);
                }
            }
        }

        for (Integer datum : data) {
            System.out.println(datum);
        }
    }

    //选择排序
    @Test
    public void test22() {
        ArrayList<Integer> data = getData();
        if(data.size() <= 2) {
            return;
        }

        for (int i = 0; i < data.size(); i++) {
            Integer min = data.get(i);
            int index = 0;
            for (int j = i + 1; j < data.size(); j++) {
                if(data.get(j) < min) {
                    min = data.get(j);
                    index = j;
                }
            }
            if(min != data.get(i)) {
                Integer num = data.get(i);
                data.remove(i);
                data.add(i, min);
                data.remove(index);
                data.add(index, num);
            }
        }

        for (Integer datum : data) {
            System.out.println(datum);
        }
    }
}
