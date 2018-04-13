package com.octo.batch.config;

import java.util.Arrays;

import com.octo.batch.Customer;
import com.octo.batch.processor.BirthdayFilterProcessor;
import com.octo.batch.processor.TransactionValidatingProcessor;
import com.octo.batch.reader.CustomerItemReader;
import com.octo.batch.writer.CustomerItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
public class BatchJobConfig {

    public static final String TASKLET_STEP = "taskletStep";

    public static final String XML_FILE = "src/main/resources/database.xml";

    private static final String JOB_NAME = "customerReportJob";

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobBuilderFactory jobBuilders;

    @Autowired
    private StepBuilderFactory stepBuilders;

    @Autowired
    private JobExplorer jobs;

    @Scheduled(fixedRate = 5000)
    public void run() throws Exception {
        JobExecution execution = jobLauncher.run(
            customerReportJob(),
            new JobParametersBuilder().addLong("uniqueness", System.nanoTime()).toJobParameters()
        );
    }

    @Bean
    public Job customerReportJob() {
        return jobBuilders.get(JOB_NAME)
            .start(chunkStep())
            .build();
    }

    @Bean
    public Step chunkStep() {
        return stepBuilders.get("chunkStep")
            .<Customer, Customer>chunk(20)
            .reader(reader()) // read from xml
            .processor(processor()) // filter birthday and transactions
            .writer(writer()) // write to txt
            .build();
    }

    // XML file reader

    @StepScope
    @Bean
    public ItemReader<Customer> reader() {
        return new CustomerItemReader(XML_FILE);
    }



    // Processors and filters

    @StepScope
    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        final CompositeItemProcessor<Customer, Customer> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(birthdayFilterProcessor(),
                transactionValidatingProcessor()));
        return processor;
    }

    @StepScope
    @Bean
    public BirthdayFilterProcessor birthdayFilterProcessor() {
        return new BirthdayFilterProcessor();
    }

    @StepScope
    @Bean
    public TransactionValidatingProcessor transactionValidatingProcessor() {
        return new TransactionValidatingProcessor(5);
    }


    // Writers

    @StepScope
    @Bean
    public ItemWriter<Customer> writer() {
        return new CustomerItemWriter();
    }


}
