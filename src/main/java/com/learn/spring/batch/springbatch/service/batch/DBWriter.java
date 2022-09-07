package com.learn.spring.batch.springbatch.service.batch;


import com.learn.spring.batch.springbatch.model.Info;
import com.learn.spring.batch.springbatch.repo.UserRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@Slf4j
public class DBWriter implements ItemWriter<Info> {

    private final AtomicInteger count = new AtomicInteger();

    @Autowired
    private  UserRepository userRepository;



    @Override
    public void write(List<? extends Info> users) throws Exception{
        System.out.println("Data Saved for Users: " + users);
        userRepository.saveAll(users);
        log.info("Chunk stored in db: " + count.getAndIncrement());
    }
}
