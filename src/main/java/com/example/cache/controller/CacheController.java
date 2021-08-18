package com.example.cache.controller;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cache")
public class CacheController {

    @GetMapping("/add")
    @CachePut(value = "redisCaffeine",key="#key")
    public String put(String key){
        System.out.println(key);
        return key;
    }

    @GetMapping("/query")
    @Cacheable(value = "redisCaffeine",key="#key")
    public String query(String key){
        System.out.println(key);
        return key;
    }

    @GetMapping("/del")
    @CacheEvict(value = "redisCaffeine",key="#key")
    public String del(String key){
        System.out.println("controller:"+key);
        return key;
    }

    @GetMapping("/delall")
    @CacheEvict(value = "redisCaffeine",allEntries = true)
    public String delall(){
        System.out.println("delall");
        return "delall";
    }

}
