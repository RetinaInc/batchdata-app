package com.example.batchdataapp.config;

import com.example.batchdataapp.entity.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingGateway;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
public class IntegrationConfig {

    //create channel
    @Bean
    MessageChannel files() {
        return MessageChannels.direct().get();
    }

    @Bean
    IntegrationFlow batchJobFlow(
            Job job,
            JdbcTemplate jdbcTemplate,
            JobLauncher launcher,
            MessageChannel files
    ) {
        return IntegrationFlows.from(files)
        .transform(
                (GenericTransformer<File, JobLaunchRequest>) file -> {
                    JobParameters jp = new JobParametersBuilder()
                            .addString("file", file.getAbsolutePath())
                            .toJobParameters();
                    return new JobLaunchRequest(job, jp);
                }
        )
//                .transform(
//                        new GenericTransformer<File, JobLaunchRequest>() {
//                            @Override
//                            public JobLaunchRequest transform(File file) {
//                                JobParameters jp = new JobParametersBuilder()
//                                        .addString("file", file.getAbsolutePath())
//                                        .toJobParameters();
//                                return new JobLaunchRequest(job, jp);
//                            }
//                        }
//                )
                .handle(new JobLaunchingGateway(launcher))
                .handle(JobExecution.class, (filePayload, messageHeaders) -> {
                            System.out.println("Job Execution from Integration Config: "+filePayload.getExitStatus().toString());
                            List<Person> personList = jdbcTemplate.query("select * from PEOPLE", (rs, rowNum) -> new Person(rs.getString("first"),
                                    rs.getString("last"),
                                    rs.getString("email")
                            ));
                            personList.forEach(System.out::println);
                            return null;
                        }

                )
                .get();
//                .handle(File.class, (filePayload, messageHeaders) -> {
//                    System.out.println("We have seen "+filePayload.getAbsolutePath());
//                    return null;
//                }).get();

//                .handle(File.class, new GenericHandler<File>() {
//                    @Override
//                    public Object handle(File filePayload, MessageHeaders messageHeaders) {
//                        System.out.println("We have seen "+filePayload.getAbsolutePath());
//                        return null;
//                    }
//                })
    }
    @Bean
    IntegrationFlow incomingFiles(
            //@Value("${HOME}/Desktop/in") File dir
            @Value("/Users/G3/Desktop/in") File dir
            ) {
        return IntegrationFlows.from(
                Files.inboundAdapter(dir)
                .preventDuplicates(true)
                .autoCreateDirectory(true),
                poller -> poller.poller(spec -> spec.fixedRate(1, TimeUnit.SECONDS)))
                .channel(this.files())
                .get();
    }
    /*

    @Bean
    IntegrationFlow incomingFiles(
            @Value("/Users/G3/Desktop/in") File dir,
            Job job,
            JdbcTemplate jdbcTemplate,
            JobLauncher launcher
            ) {
        return IntegrationFlows.from(
                Files.inboundAdapter(dir)
                .preventDuplicates(true)
                .autoCreateDirectory(true),
                poller -> poller.poller(spec -> spec.fixedRate(1, TimeUnit.SECONDS)))
                .transform(
                        (GenericTransformer<File, JobLaunchRequest>) file -> {
                            JobParameters jp = new JobParametersBuilder()
                                    .addString("file", file.getAbsolutePath())
                                    .toJobParameters();
                            return new JobLaunchRequest(job, jp);
                        }
                )
//                .transform(
//                        new GenericTransformer<File, JobLaunchRequest>() {
//                            @Override
//                            public JobLaunchRequest transform(File file) {
//                                JobParameters jp = new JobParametersBuilder()
//                                        .addString("file", file.getAbsolutePath())
//                                        .toJobParameters();
//                                return new JobLaunchRequest(job, jp);
//                            }
//                        }
//                )
                .handle(new JobLaunchingGateway(launcher))
                .handle(JobExecution.class, (filePayload, messageHeaders) -> {
                    System.out.println("Job Execution from Integration Config: "+filePayload.getExitStatus().toString());
                            List<Person> personList = jdbcTemplate.query("select * from PEOPLE", (rs, rowNum) -> new Person(rs.getString("first"),
                                    rs.getString("last"),
                                    rs.getString("email")
                            ));
                            personList.forEach(System.out::println);
                    return null;
                }

                )
                .get();
//                .handle(File.class, (filePayload, messageHeaders) -> {
//                    System.out.println("We have seen "+filePayload.getAbsolutePath());
//                    return null;
//                }).get();

//                .handle(File.class, new GenericHandler<File>() {
//                    @Override
//                    public Object handle(File filePayload, MessageHeaders messageHeaders) {
//                        System.out.println("We have seen "+filePayload.getAbsolutePath());
//                        return null;
//                    }
//                })

    }
     */
}
