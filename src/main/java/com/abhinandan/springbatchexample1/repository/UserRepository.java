package com.abhinandan.springbatchexample1.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.abhinandan.springbatchexample1.model.User;

public interface UserRepository extends JpaRepository<User, Integer> {
}
