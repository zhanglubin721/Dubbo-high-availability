package com.yicaida.consumer.controller;

import com.alibaba.dubbo.config.ApplicationConfig;
import com.alibaba.dubbo.config.ReferenceConfig;
import com.alibaba.dubbo.config.annotation.Reference;
import com.alibaba.dubbo.rpc.service.GenericService;
import com.netflix.hystrix.contrib.javanica.annotation.HystrixCommand;
import com.yicaida.projectAPI.pojo.Student;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import com.yicaida.projectAPI.pojo.User;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;
import org.springframework.web.servlet.support.RequestContextUtils;
import service.UserService;

import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController()
@RequestMapping("testController")
public class TestController {

    @Reference(loadbalance = "roundrobin", version = "1.0.1", group = "alibaba")
    private UserService userService;


    @RequestMapping("test1")
    public List<User> test1(HttpServletRequest httpRequest, RedirectAttributes redirectAttributes) {
        HttpSession session = httpRequest.getSession();
        String test = (String)session.getAttribute("bbb");
        Cookie[] cookies = httpRequest.getCookies();
        System.out.println(test);
        return null;
//        return userService.findAllUser();
    }

    @HystrixCommand(fallbackMethod = "aaa")
    @RequestMapping("findData")
    public List<Student> findData() {
        //直接调用
        return userService.findData();
    }

    @HystrixCommand(fallbackMethod = "aaa")
    @RequestMapping("findData2")
    public List<Student> findData2() {
        //泛化调用
        ReferenceConfig<GenericService> reference = new ReferenceConfig<>();
        reference.setApplication(new ApplicationConfig("provider"));
        reference.setGroup("alibaba");
        reference.setVersion("1.0.1");
        reference.setInterface("service.UserService");
        reference.setUrl("dubbo://127.0.0.1:20880?scope=remote");
        reference.setGeneric(true);
        GenericService genericService = reference.get();
        genericService.$invoke("findData", new String[]{}, new Object[]{});
        return null;
    }

    @RequestMapping("getAllTable")
    public List<Map<String, Object>> getAllTable() throws Exception {
        return userService.getAllTable();
    }

    @RequestMapping("testRedis")
    public void testRedis() {
        userService.testRedis();
    }

    @RequestMapping("testapp")
    public Map<String, Object> testapp(String productid, String hello) {
        System.out.println("调用成功");
        System.out.println("productid: " + productid);
        System.out.println("hello: " + hello);
        HashMap<String, Object> stringObjectHashMap = new HashMap<>();
        stringObjectHashMap.put("数据", "aaa");
        return stringObjectHashMap;
    }

    public List<Student> aaa() {
        System.out.println("进入熔断");
        return null;
    }


}
