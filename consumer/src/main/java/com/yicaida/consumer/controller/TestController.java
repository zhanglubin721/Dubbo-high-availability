package com.yicaida.consumer.controller;

import com.alibaba.dubbo.config.annotation.Reference;
import com.yicaida.projectAPI.pojo.Student;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.yicaida.projectAPI.pojo.User;
import service.UserService;

import java.util.List;
import java.util.Map;

@RestController()
@RequestMapping("testController")
public class TestController {

    @Reference
    private UserService userService;

    private  NoticeBroadcast noticeBroadcast;

    @RequestMapping("test1")
    public List<User> test1() {
        return userService.findAllUser();
    }

    @RequestMapping("findData")
    public List<Student> findData() {
        return userService.findData();
    }

    @RequestMapping("getAllTable")
    public List<Map<String, Object>> getAllTable() throws Exception {
        return userService.getAllTable();
    }

    @RequestMapping("test2")
    public void test2() throws Exception {
        noticeBroadcast = new NoticeBroadcast(10001);
        noticeBroadcast.run();
    }

    @RequestMapping("test3")
    public void test3() throws Exception {
        noticeBroadcast.stop();
    }

    @RequestMapping("testRedis")
    public void testRedis() {
        userService.testRedis();
    }

}
