package com.learn.spring.batch.springbatch.service.batch;


import com.learn.spring.batch.springbatch.model.Info;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component
public class Processor implements ItemProcessor<Info, Info> {

    private static final Map<String, String> DEPT_NAMES =
            new HashMap<>();

    public Processor() {
        DEPT_NAMES.put("1", "Technology");
        DEPT_NAMES.put("2", "Operations");
        DEPT_NAMES.put("3", "Accounts");
    }

    @Override
    public Info process(Info info) throws Exception {
        String deptCode = info.getDept();
        String dept = DEPT_NAMES.get(deptCode);
        info.setDept(dept);
        info.setTime(new Date());
        System.out.println(String.format("Converted from [%s] to [%s]", deptCode, dept));
        return info;
    }
}
