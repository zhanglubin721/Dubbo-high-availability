package com.yicaida.projectAPI.pojo;

public class Number {
    public synchronized void getOne(){//Number.class
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {

        }
        System.out.println("one");
    }
    public synchronized void getTwo(){//this
        System.out.println("two");
    }
    public void getThree(){
        System.out.println("three");
    }
}
