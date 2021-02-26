package com.yicaida.consumer.controller;

import com.alibaba.dubbo.config.annotation.Reference;
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
import java.util.List;
import java.util.Map;

@RestController()
@RequestMapping("testController")
public class TestController {

    @Reference
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

    @RequestMapping("findData")
    public List<Student> findData() {
        return userService.findData();
    }

    @RequestMapping("getAllTable")
    public List<Map<String, Object>> getAllTable() throws Exception {
        return userService.getAllTable();
    }

    @RequestMapping("testRedis")
    public void testRedis() {
        userService.testRedis();
    }

}
