package com.fastcampus.pass.repository.pass;

import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDateTime;
import java.util.List;

public interface BulkPassRepository extends JpaRepository<BulkPassEntity, Integer> {

    // where status = : status and startedAt > :startedAt
    List<BulkPassEntity> findByStatusAndStartedAtGreaterThan(BulkPassStatus status, LocalDateTime startedAt);
}
