package com.yicaida.consumer.controller;

import com.alibaba.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.yicaida.projectAPI.pojo.User;
import service.UserService;

import java.util.List;

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

    @RequestMapping("test2")
    public void test2() throws Exception {
        noticeBroadcast = new NoticeBroadcast(10001);
        noticeBroadcast.run();
    }

    //测试
    //测试4
    //测试2
    //测试3
    @RequestMapping("test3")
    public void test3() throws Exception {
        noticeBroadcast.stop();
    }
    //111
    //22
    //33


}
