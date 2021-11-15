package com.yicaida.provider.service.impl;

import com.yicaida.projectAPI.pojo.Student;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

public class TestConfig {

    public TestConfig() {
        System.out.println("配置文件已加载");
    }

    @Bean(name = "student")
    public static Student student() {
        return new Student();
    }
}
