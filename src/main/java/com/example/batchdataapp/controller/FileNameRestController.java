package com.example.batchdataapp.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

import java.io.File;

@RestController
public class FileNameRestController {

    private final MessageChannel files;


    public FileNameRestController(MessageChannel files) {
        this.files = files;
    }

    @RequestMapping(method= RequestMethod.GET, value ="/files")
    void  triggerJobForFile(@RequestParam String file) {
        Message<File> fileMessage = MessageBuilder.withPayload(new File(file))
                .build();
        this.files.send(fileMessage);
    }


}
