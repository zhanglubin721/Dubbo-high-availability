package com.yicaida.provider.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.fdd.api.client.constant.ApiUrlConstant;
import org.apache.jute.Index;
import org.junit.Test;
import org.springframework.util.StringUtils;

import java.sql.Array;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * @author zhanglubin
 * @date 2021/6/15
 */
public class TestService {

    public static Integer a = 3;

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
        for (int i = 0; i < datas.length - 1; i++) {
            temporary = datas[i + 1];
            int j;
            for (j = i + 1; j > 0 && j < datas.length && temporary < datas[j - 1]; j--) {
                datas[j] = datas[j - 1];
            }
            datas[j] = temporary;
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
        for (int i = 0; i < datas.size() - 1; i++) {
            temporary = datas.get(i + 1);
            int j;
            for (j = i + 1; j > 0 && j < datas.size() && temporary < datas.get(j - 1); j--) {
                datas.set(j, datas.get(j - 1));
            }
            datas.set(j, temporary);
        }
        for (Integer data : datas) {
            System.out.println(data);
        }
    }

    @Test
    public void test4() {
        int[] datas = initDataList();
        int value;

        for (int temp = datas.length / 2; temp > 0; temp /= 2) {
            for (int i = temp; i < datas.length; i++) {
                value = datas[i];
                int j;
                for (j = i - temp; j >= 0 && datas[j] > value; j -= temp) {
                    datas[j + temp] = datas[j];
                }
                datas[j + temp] = value;
            }
        }

        for (int data : datas) {
            System.out.println(data);
        }
    }

    @Test
    public void test99() {
        ReentrantLock reentrantLock = new ReentrantLock();
        try {
            reentrantLock.lock();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            reentrantLock.unlock();
        }
    }


    @Test
    public void testjjj() {
        ArrayList<String> aaa = new ArrayList<>();
        aaa.add("a");
        aaa.add("b");
        aaa.add("c");
        aaa.add("d");
        aaa.add("e");
        aaa.stream().filter(string -> "a".equals(string)).collect(Collectors.toList());
        for (String s : aaa) {
            System.out.println(s);
        }
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


    @Test
    public void test1111() {
        Integer integer1 = 127;
        Integer integer2 = 127;
        if (integer1 == integer2) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }

        ArrayList<String> strings = new ArrayList<>();
        strings.add("123");
        strings.add("234");
        String s = JSONObject.toJSONString(strings);
        ArrayList arrayList = JSONArray.parseObject(s, ArrayList.class);
        for (Object o : arrayList) {
            System.out.println(o);
        }
    }

    @Test
    public void test1154() {
        int[] nums1 = new int[]{1,3,4,9};
        int[] nums2 = new int[]{1,2,3,4,5,6,7,8,9};
        double a =  Stream.concat(Arrays.stream(nums1).boxed(), Arrays.stream(nums2).boxed())
                .sorted()
                .limit((nums1.length + nums2.length) / 2 + 1)
                .skip((nums1.length + nums2.length) % 2 != 0 ?
                        (nums1.length + nums2.length) / 2 :
                        (nums1.length + nums2.length) / 2 - 1)
                .collect(Collectors.averagingInt(Integer::intValue));
        System.out.println(a);
    }

    public void test342() {
        String s = new String("23");
        int strLength = s.length();

        HashMap<String, String> stringListHashMap = new HashMap<>();
        stringListHashMap.put("2", "abc");
        stringListHashMap.put("3", "def");
        stringListHashMap.put("4", "ghi");
        stringListHashMap.put("5", "jkl");
        stringListHashMap.put("6", "mno");
        stringListHashMap.put("7", "pqrs");
        stringListHashMap.put("8", "tuv");
        stringListHashMap.put("9", "wxyz");

        int groupSum = 0;
        ArrayList<String> strings = new ArrayList<>();
        for (int i = 0; i < strLength; i++) {
            StringBuilder stringBuilder = new StringBuilder();
            char c = s.charAt(i);

        }
    }

    @Test
    public void test8948() {
        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            integers.add(i);
        }
        //多管道遍历
        CopyOnWriteArrayList integerList = new CopyOnWriteArrayList();
        long startTime = System.currentTimeMillis();
        integers.parallelStream().forEach(e -> {
            //添加list的方法
            integerList.add(e);
            try {
                //休眠100ms，假装执行某些任务
                Thread.sleep(10);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        });
        long endTime = System.currentTimeMillis();
        System.out.println(integerList.size());
        System.out.println(endTime - startTime);
    }

    @Test
    public void test8rr948() {
        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < 100; i++){
            integers.add(i);
        }
        //多管道遍历
        ArrayList integerList = new ArrayList();
        long startTime = System.currentTimeMillis();
        integers.stream().forEach(e -> {
            //添加list的方法
            integerList.add(e);
            try {
                //休眠100ms，假装执行某些任务
                Thread.sleep(10);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        });
        long endTime = System.currentTimeMillis();
        System.out.println(integerList.size());
        System.out.println(endTime - startTime);
    }

    //2021-10-12
    public class ListNode {
        int val;
        ListNode next;
        ListNode() {}
        ListNode(int val) {
            this.val = val;
        }
        ListNode(int val, ListNode next) {
            this.val = val; this.next = next;
        }
    }

    public int removeDuplicates(int[] nums) {
        int length = 1;
        if (nums.length == 0) {
            return 0;
        }
        int arrVal = nums[0];
        for (int startIndex = 1; startIndex < nums.length; startIndex++) {
            for (int endIndex = startIndex; endIndex < nums.length; endIndex++) {
                if (nums[endIndex] != arrVal && nums[endIndex] > arrVal) {
                    nums[startIndex] = nums[endIndex];
                    arrVal = nums[startIndex];
                    length++;
                    break;
                }
            }
        }
        return length;
    }

    @Test
    public void removeElement() {
        int[] nums = {0,1,2,2,3,0,4,2};
        int val = 2;

        int rightIndex = 0;
        int leftIndex = 0;
        int length = 0;

        while (rightIndex < nums.length) {
            if (nums[rightIndex] == val) {
                rightIndex++;
            } else {
                nums[leftIndex] = nums[rightIndex];
                rightIndex++;
                leftIndex++;
                length++;
            }
        }
        IntStream.range(0, length).forEach(Index -> {
            System.out.println(nums[Index]);
        });
        System.out.println(length);
    }

    @Test
    public void test () {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("aaa");
        strings.add("bbb");
        //类转json字符串
        String jsonString = JSON.toJSONString(strings);
        //json字符串转想要的bean类
        Object o = JSON.parseObject(jsonString, Object.class);

    }

    @Test
    public void tes1111t() {
        ArrayList<String> list = new ArrayList<String>();
        list.add("a");
        list.add("b");
        list.add("c");
        list.add("d");
        System.out.println(list.subList(0, 2));
    }



}
