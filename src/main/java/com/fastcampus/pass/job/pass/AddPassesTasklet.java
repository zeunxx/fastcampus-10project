package com.fastcampus.pass.job.pass;

import com.fastcampus.pass.repository.pass.*;
import com.fastcampus.pass.repository.user.UserGroupMappingEntity;
import com.fastcampus.pass.repository.user.UserGroupMappingRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

@Slf4j
@Component
@RequiredArgsConstructor
public class AddPassesTasklet implements Tasklet {
    private final PassRepository passRepository;
    private final BulkPassRepository bulkPassRepository;
    private final UserGroupMappingRepository userGroupMappingRepository;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        // 이용권 시작 일시 1일 전 user group 내 각 사용자에게 이용권 추가
        final LocalDateTime startedAt = LocalDateTime.now().minusDays(1);
        final List<BulkPassEntity> bulkPassEntities = bulkPassRepository.findByStatusAndStartedAtGreaterThan(BulkPassStatus.READY, startedAt);

        int count = 0;
        // 대량 이용권 정보를 돌면서 user group에 속한 userId를 조회하고 해당 userId로 이용권 추가
        for (BulkPassEntity bulkPassEntity : bulkPassEntities) {
            final List<String> userIds = userGroupMappingRepository.findByUserGroupId(bulkPassEntity.getUserGroupId())
                    .stream().map(UserGroupMappingEntity::getUserId).toList();
            count+=addPasses(bulkPassEntity, userIds); // 추가한 유저 건수

            bulkPassEntity.setStatus(BulkPassStatus.COMPLETED);
        }


        log.info("AddPassesTasklet - execute: 이용권 {}건 추가 완료, startedAt={}", count, startedAt);
        return RepeatStatus.FINISHED;
    }

    // bulkPass의 정보로 pass 데이터 생성
    private int addPasses(BulkPassEntity bulkPassEntity, List<String> userIds){
        List<PassEntity> passEntities = new ArrayList<>();

        for (String userId : userIds) {
            PassEntity passEntity = PassModelMapper.INSTANCE.toPassEntity(bulkPassEntity, userId); // 그냥 setter로 구현할 수 있음
            passEntities.add(passEntity);
        }
        return passRepository.saveAll(passEntities).size();

    }
}
