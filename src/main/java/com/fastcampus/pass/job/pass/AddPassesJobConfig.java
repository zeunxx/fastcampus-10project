package com.fastcampus.pass.job.pass;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class AddPassesJobConfig {

    // @EnableBatchProcessing으로 인해 Bean으로 제공된 JobBuilderFactory, StepBuilderFactory
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final AddPassesTasklet addPassesTasklet; // tasklet을 따로 클래스로 선언해 item reader~writer 부분 해당 클래스에 작성

    @Bean
    public Job addPassesJob(){
        return this.jobBuilderFactory.get("addPassesJob")
                .start(addPassesStep())
                .build();
    }

    @Bean
    public Step addPassesStep(){
        return this.stepBuilderFactory.get("addPassesStep")
                .tasklet(addPassesTasklet) // chunk 선언 대신 tasklet 사용
                .build();
    }

}
