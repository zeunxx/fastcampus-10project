package com.fastcampus.pass.job.notification;

import com.fastcampus.pass.repository.notification.NotificationEntity;
import com.fastcampus.pass.repository.notification.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import org.springframework.batch.item.ItemWriter;
import java.util.List;

import static java.time.LocalDateTime.now;

@Slf4j
@Component
@RequiredArgsConstructor
public class SendNotificationItemWriter implements ItemWriter<NotificationEntity> {

    private final NotificationRepository notificationRepository;
//    private final KakaoTalkMessageAdapter kakaoTalkMessageAdapter;

    @Override
    public void write(List<? extends NotificationEntity> notificationEntities) throws Exception {
        int count = 0;

        boolean successful = true;
        for (NotificationEntity notificationEntity : notificationEntities) {
//            boolean successful = kakaoTalkMessageAdapter.sendKakaoTalkMessage(notificationEntity.getUuid(), notificationEntity.getText());
            if(successful){
                notificationEntity.setSent(true);
                notificationEntity.setSentAt(now());
                notificationRepository.save(notificationEntity);
                count++;
            }
        }
        log.info("SendNotificationItemWriter - write: 수업 전 알람 {}/{}건 전송 성공", count, notificationEntities.size());
    }
}
