package com.learn.spring.batch.springbatch.repo;

import com.learn.spring.batch.springbatch.model.Info;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepository extends JpaRepository<Info, Integer> {
}
