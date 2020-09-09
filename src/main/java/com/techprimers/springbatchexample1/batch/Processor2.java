 package com.techprimers.springbatchexample1.batch;

import org.springframework.batch.item.ItemProcessor;

import com.techprimers.springbatchexample1.model.User;

public class Processor2 implements ItemProcessor<User,User>{

	@Override
	public User process(User user) throws Exception {
		return user;
	}
 
}
