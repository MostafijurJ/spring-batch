package com.learn.spring.batch.springbatch.config;


import com.learn.spring.batch.springbatch.model.Info;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.io.FileNotFoundException;

@Configuration
@EnableBatchProcessing
@Slf4j
public class SpringBatchConfig {

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory,
                   StepBuilderFactory stepBuilderFactory,
                   ItemReader<Info> itemReader,
                   ItemProcessor<Info, Info> itemProcessor,
                   ItemWriter<Info> itemWriter
    ) {

        Step step = stepBuilderFactory.get("ETL-file-load")
            .<Info, Info>chunk(10)
            .reader(itemReader)
            .processor(itemProcessor)
            .writer(itemWriter)
            .faultTolerant()
            .skipLimit(10)
            .skip(Exception.class)
            .noSkip(FileNotFoundException.class)
            .build();


        return jobBuilderFactory.get("ETL-Load")
            .incrementer(new RunIdIncrementer())
            .start(step)
            .build();
    }

    @Bean
    public FlatFileItemReader<Info> itemReader() {
        FlatFileItemReader<Info> flatFileItemReader = new FlatFileItemReader<>();

        try{
            flatFileItemReader.setResource(new FileSystemResource("src/main/resources/info.csv"));
            flatFileItemReader.setName("CSV-Reader");
            //TODO in strict mode reader must need something to read
            flatFileItemReader.setStrict(false);
            flatFileItemReader.setLinesToSkip(1);
            flatFileItemReader.setLineMapper(lineMapper());
        }catch (Exception e){
            log.error("Exception occurred due to file reading "+e);
        }
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<Info> lineMapper() {

        DefaultLineMapper<Info> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("id", "name", "dept", "salary");

        BeanWrapperFieldSetMapper<Info> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Info.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }

}
