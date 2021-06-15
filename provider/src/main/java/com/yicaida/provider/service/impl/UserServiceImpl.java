package com.yicaida.provider.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import com.yicaida.projectAPI.pojo.Student;
import com.yicaida.provider.mapper.UserMapper;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import com.yicaida.projectAPI.pojo.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import service.UserService;

import javax.swing.tree.TreeNode;
import java.sql.Array;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service(version = "1.0.1", group = "alibaba")
public class UserServiceImpl implements UserService {

    @Value("${server.port}")
    private String port;

    @Autowired
    private UserMapper userMapper;

    @Autowired
    private RedisTemplate redisTemplate;
    /**
     * @Author: zlb
     * @Date: 16:35 2020/11/19
     * @Description: 查询所有用户
     */
    @Override
    public List<User> findAllUser() {
        return userMapper.findAllUser();
    }

    @Override
    public List<Student> findData() {
//        return userMapper.findData();
        System.out.println(port);
        return null;
    }

    @Override
    public List<Map<String, Object>> getAllTable() {
        return userMapper.getAllTable();
    }

    @Override
    public void testRedis() {
        Boolean aBoolean = redisTemplate.hasKey("test");
        if (aBoolean) {
            redisTemplate.opsForValue().get("test");
            System.out.println("查询到test，直接返回");
        } else {
            redisTemplate.opsForValue().set("test", "testbb");
            redisTemplate.expire("test", 30, TimeUnit.MINUTES);
            System.out.println("未查询到test，新建key");
        }
    }

    //测试jdk8新特性 stream流
//    @Test
//    public void test1() {
//        User user1 = new User();
//        User user2 = new User();
//        User user3 = new User();
//        User user4 = new User();
//        User user5 = new User();
//        User user6 = new User();
//        user1.setName("1");
//        user2.setName("2");
//        user3.setName("3");
//        user4.setName("4");
//        user5.setName("5");
//        user6.setName("6");
//
//        ArrayList<User> users = new ArrayList<>();
//        users.add(user1);
//        users.add(user2);
//        users.add(user3);
//        users.add(user4);
//        users.add(user5);
//        users.add(user6);
//
//        ArrayList<User> usersNameIs1 = new ArrayList<>();
//
//        List<Student> collect = users.stream().filter(user -> {
//            return "1".equals(user.getName()) || "2".equals(user.getName());
//        }).map(user -> {
//            Student student = new Student();
//            student.setSname(user.getName());
//            return student;
//        }).collect(Collectors.toList());
//
//        for (Student student : collect) {
//            System.out.println(student.getSname());
//        }
//
//        Integer integer = new Integer(1);
//
//    }

//    @Test
//    public void test2() {
//        String a = "test";
//        String b = "test";
//        String c = new String("test");
//        if (a == b) {
//            System.out.println("a == b " + true);
//        }
//        if (a == c) {
//            System.out.println("a == c " + true);
//        } else {
//            System.out.println("a == c " + false);
//        }
//
//        int i = "Aa".hashCode();
//        int j = "BB".hashCode();
//        System.out.println(i);
//        System.out.println(j);
//
//        HashMap<Object, Object> objectObjectHashMap = new HashMap<>();
//        objectObjectHashMap.put("Aa", "1");
//        objectObjectHashMap.put("BB", "2");
//        Set<Map.Entry<Object, Object>> entries = objectObjectHashMap.entrySet();
//        for (Map.Entry<Object, Object> entry : entries) {
//            System.out.println(entry.getKey());
//            System.out.println(entry.getValue());
//        }
//    }

}
