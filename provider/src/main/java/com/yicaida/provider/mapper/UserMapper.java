package com.yicaida.provider.mapper;

import com.yicaida.projectAPI.pojo.Student;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;
import com.yicaida.projectAPI.pojo.User;

import java.util.List;

@Repository
@Mapper
public interface UserMapper {

    List<User> findAllUser();

    List<Student> findData();
}
