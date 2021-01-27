package service;


import com.yicaida.projectAPI.pojo.Student;
import com.yicaida.projectAPI.pojo.User;

import java.util.List;
import java.util.Map;

public interface UserService {

    List<User> findAllUser();

    List<Student> findData();

    List<Map<String, Object>> getAllTable();

    void testRedis();
}
