package com.example.batch;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@SpringBootApplication
public class BatchApplication {
    public static void main(String[] args) {
        var context = SpringApplication.run(BatchApplication.class, args);
        System.exit(SpringApplication.exit(context));
    }

    @Bean
    Job exposureJob(JobRepository repository, Step loadTrades, Step calculateExposure, Step reconcile) {
        return new JobBuilder("exposureJob", repository).start(loadTrades).next(calculateExposure).next(reconcile).build();
    }

    @Bean
    Step loadTrades(JobRepository repository, PlatformTransactionManager tx, JdbcTemplate jdbc) {
        return new StepBuilder("loadTrades", repository).tasklet((contribution, context) -> {
            String date = context.getStepContext().getJobParameters().get("businessDate").toString();
            jdbc.execute("CREATE SCHEMA IF NOT EXISTS spring_poc");
            jdbc.execute("CREATE TABLE IF NOT EXISTS spring_poc.trades (business_date date, trade_id text, tree_id text, amount numeric(20,2), PRIMARY KEY(business_date,trade_id))");
            jdbc.execute("CREATE TABLE IF NOT EXISTS spring_poc.exposures (business_date date, tree_id text, amount numeric(20,2), PRIMARY KEY(business_date,tree_id))");
            try (var stream = new ClassPathResource("trades.csv").getInputStream()) {
                for (String line : new String(stream.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                    if (line.isBlank() || line.startsWith("trade_id")) continue;
                    var row = line.split(",");
                    jdbc.update("INSERT INTO spring_poc.trades VALUES (?::date,?,?,?) ON CONFLICT (business_date,trade_id) DO UPDATE SET tree_id=excluded.tree_id,amount=excluded.amount", date,row[0],row[1],new BigDecimal(row[2]));
                }
            }
            return RepeatStatus.FINISHED;
        }, tx).build();
    }

    @Bean
    Step calculateExposure(JobRepository repository, PlatformTransactionManager tx, JdbcTemplate jdbc) {
        return new StepBuilder("calculateExposure", repository).tasklet((contribution, context) -> {
            var parameters = context.getStepContext().getJobParameters();
            if ("true".equals(parameters.get("forceFailure"))) throw new IllegalStateException("Requested POC failure");
            jdbc.update("INSERT INTO spring_poc.exposures SELECT business_date,tree_id,sum(amount)*1.10 FROM spring_poc.trades WHERE business_date=?::date GROUP BY business_date,tree_id ON CONFLICT (business_date,tree_id) DO UPDATE SET amount=excluded.amount", parameters.get("businessDate"));
            return RepeatStatus.FINISHED;
        }, tx).build();
    }

    @Bean
    Step reconcile(JobRepository repository, PlatformTransactionManager tx, JdbcTemplate jdbc) {
        return new StepBuilder("reconcile", repository).tasklet((contribution, context) -> {
            var date = context.getStepContext().getJobParameters().get("businessDate");
            var rows = jdbc.queryForList("SELECT tree_id,amount FROM spring_poc.exposures WHERE business_date=?::date ORDER BY tree_id", date);
            if (rows.size()!=2 || ((BigDecimal)rows.get(0).get("amount")).compareTo(new BigDecimal("385.00"))!=0 || ((BigDecimal)rows.get(1).get("amount")).compareTo(new BigDecimal("55.00"))!=0) throw new IllegalStateException("Reconciliation failed: "+rows);
            System.out.println("POC_RECONCILED businessDate="+date+" results="+rows);
            return RepeatStatus.FINISHED;
        }, tx).build();
    }
}
