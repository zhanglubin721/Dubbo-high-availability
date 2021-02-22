package com.yicaida.provider;

import com.yicaida.projectAPI.pojo.User;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class ZlbTest2 {

    @Test
    public void test1() throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class c = Class.forName("com.yicaida.projectAPI.pojo.User");
        Constructor constructor = c.getConstructor(Long.class, String.class);
        User zlb = (User)constructor.newInstance(111l, "zlb");
        System.out.println(zlb.getName());
    }
}
