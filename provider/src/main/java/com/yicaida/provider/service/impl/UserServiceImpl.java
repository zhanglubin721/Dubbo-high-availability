package com.yicaida.provider.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.yicaida.projectAPI.pojo.Student;
import com.yicaida.provider.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import com.yicaida.projectAPI.pojo.User;
import org.springframework.data.redis.core.RedisTemplate;
import service.UserService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class UserServiceImpl implements UserService {

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
}
