package org.example.controller;

import org.example.MainTreeLSM;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataController {

    @Autowired
    private MainTreeLSM mainTreeLSM;

    @GetMapping("/getKey")
    public Integer getValueForKey(@RequestParam("key") int key){
        return mainTreeLSM.searchKey(key);
    }
}
