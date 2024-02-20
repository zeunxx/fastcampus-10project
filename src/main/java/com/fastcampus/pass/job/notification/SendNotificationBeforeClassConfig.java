package com.fastcampus.pass.job.notification;

import com.fastcampus.pass.repository.booking.BookingEntity;
import com.fastcampus.pass.repository.notification.NotificationEntity;
import com.fastcampus.pass.repository.notification.NotificationEvent;
import com.fastcampus.pass.repository.notification.NotificationModelMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.persistence.EntityManagerFactory;
import java.awt.print.Book;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class SendNotificationBeforeClassConfig {
    private final int CHUNK_SIZE = 10;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private final SendNotificationItemWriter sendNotificationItemWriter;

    @Bean
    public Job sendNotificationBeforeClassJob(){
        return this.jobBuilderFactory.get("sendNotificationBeforeClassJob")
                .start(addNotificationStep()) // first step
                .next(sendNotificationStep()) // second step
                .build();
    }

    @Bean
    public Step addNotificationStep(){
        return this.stepBuilderFactory.get("addNotificationStep")
                .<BookingEntity, NotificationEntity> chunk(CHUNK_SIZE) // input: 예약정보, output: 알림정보
                .reader(addNotificationItemReader())
                .processor(addNotificationItemProcessor())
                .writer(addNotificationItemWriter())
                .build();
    }

    /**
     * JpaPagingItemReader: JPA에서 사용하는 페이징 기법
     * 쿼리 당 page size만큼 가져오며 다른 PagingItemReader와 마찬가지로 thread-safe함
     * - 조회하는 것에 대한 업데이트가 이뤄지지 않아서 cursor를 사용할 필요 없음
     */
    @Bean
    public JpaPagingItemReader<BookingEntity> addNotificationItemReader(){
        // status가 준비중이며, startedAt이 10분 후 시작하는 예약이 알림 대상
        return  new JpaPagingItemReaderBuilder<BookingEntity>()
                .name("addNotificationItemReader")
                .entityManagerFactory(entityManagerFactory)
                .pageSize(CHUNK_SIZE)
                .queryString("select b from BookingEntity b join fetch b.userEntity where b.status = :status and b.startedAt <= :startedAt" +
                        " order by b.bookingSeq")
                .build();
    }

    @Bean
    public ItemProcessor<BookingEntity, NotificationEntity> addNotificationItemProcessor(){
        return bookingEntity -> NotificationModelMapper.INSTANCE.toNotificationEntity(bookingEntity, NotificationEvent.BEFORE_CLASS);
    }

    @Bean
    public JpaItemWriter<NotificationEntity> addNotificationItemWriter(){
        return new JpaItemWriterBuilder<NotificationEntity>()
                .entityManagerFactory(entityManagerFactory)
                .build();
    }

    @Bean
    public Step sendNotificationStep(){
        return this.stepBuilderFactory.get("sendNotificationStep")
                .<NotificationEntity, NotificationEntity> chunk(CHUNK_SIZE)
                .reader(sendNotificationItemReader())
                .writer(sendNotificationItemWriter)
                .taskExecutor(new SimpleAsyncTaskExecutor()) // thread 설정 정책에 따라 사용할지 안할지 정해짐
                .build();
    }

    // cursor 기반의 방식은 thread safe 하지 않음 (paging은 safe)
    // 따라서, 멀티스레드 환경에선 reader와 writer가 safe 한지 확인 필요
    // 이번 방식은 read 후 update가 필요하므로 cursor 방식이 필요 => SynchronizedItemStreamReader 사용해서 동기화 사용
    // = read가 순차적 실행됨, write는 멀티스레드로 진행됨
    @Bean
    public SynchronizedItemStreamReader<NotificationEntity> sendNotificationItemReader(){
        // event가 수업 전이며, 발송 여부(sent)가 미발송인 알람이 조회 대상
        JpaCursorItemReader<NotificationEntity> itemReader = new JpaCursorItemReaderBuilder<NotificationEntity>()
                .name("sendNotificationItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select n from NotificationEntity n where n.event = :event and n.sent = :sent")
                .parameterValues(Map.of("event",NotificationEvent.BEFORE_CLASS,"sent",false))
                .build();
        return new SynchronizedItemStreamReaderBuilder<NotificationEntity>()
                .delegate(itemReader)
                .build();
    }

}
