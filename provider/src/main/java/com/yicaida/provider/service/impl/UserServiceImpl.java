package com.yicaida.provider.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.yicaida.projectAPI.pojo.Student;
import com.yicaida.provider.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import com.yicaida.projectAPI.pojo.User;
import service.UserService;

import java.util.List;

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    private UserMapper userMapper;
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
        return userMapper.findData();
    }
}
