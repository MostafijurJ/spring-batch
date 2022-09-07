package com.learn.spring.batch.springbatch.config;


import com.learn.spring.batch.springbatch.model.Info;
import com.learn.spring.batch.springbatch.service.batch.DBWriter;
import com.learn.spring.batch.springbatch.service.batch.Processor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.task.TaskExecutor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;

@Configuration
@EnableBatchProcessing
@Slf4j
public class SpringBatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobExecutionListener jobExecutionListener;
    @Autowired
    private ResourcePatternResolver resourcePatternResolver;


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
    @StepScope
    public FlatFileItemReader<Info> itemReader() {
        FlatFileItemReader<Info> flatFileItemReader = new FlatFileItemReader<>();

        try {
            flatFileItemReader.setResource(new FileSystemResource("src/main/resources/info.csv"));
            flatFileItemReader.setName("CSV-Reader");
            //TODO in strict mode reader must need something to read
            flatFileItemReader.setStrict(false);
            flatFileItemReader.setLinesToSkip(1);
            flatFileItemReader.setLineMapper(lineMapper());
        } catch (Exception e) {
            log.error("Exception occurred due to file reading " + e);
        }
        return flatFileItemReader;
    }

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


    @Bean
    public Partitioner partitioner() throws IOException {
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setResources(resourcePatternResolver.getResources(" file:D:/dev/springBatchExample/sample-data*.csv"));
        return partitioner;
    }

    @Bean
    public PartitionHandler partitionHandler() throws MalformedURLException {
        TaskExecutorPartitionHandler retVal = new TaskExecutorPartitionHandler();
        retVal.setTaskExecutor(taskExecutor());
        retVal.setStep(step1());
        retVal.setGridSize(5);
        return retVal;
    }


    private TaskExecutor taskExecutor() {
        return new TaskExecutor() {
            @Override
            public void execute(Runnable task) {
                task.run();
            }
        };
    }

    @Bean
    public Step step1(){
        return stepBuilderFactory.get("step1")
            .<Info, Info>chunk(2)
            .reader(itemReader())
            .processor(new Processor())
            .writer(new DBWriter())
            .build();
    }


    @Bean
    public Step step1Manager() throws Exception {
        return stepBuilderFactory.get("step1.manager")
            .partitioner("step1", partitioner())
            .partitionHandler(partitionHandler())
            .build();
    }

    @Bean
    public Job processJob() throws Exception {
        return jobBuilderFactory.get("processJob")
            .incrementer(new RunIdIncrementer())
            .listener(jobExecutionListener)
            .start(step1Manager())
            .build();
    }


}
