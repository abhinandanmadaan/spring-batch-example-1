package com.abhinandan.springbatchexample1.classifier;

import org.springframework.batch.item.ItemWriter;

import org.springframework.classify.Classifier;

import com.abhinandan.springbatchexample1.model.User;

public class UserClassifier implements Classifier<User, ItemWriter<? super User>> {
	 
    private static final long serialVersionUID = 1L;
     
//    private ItemWriter<User> evenItemWriter;
//    private ItemWriter<User> oddItemWriter;
    
    private ItemWriter<User> operationsItemWriter;
    private ItemWriter<User> accountsItemWriter;
    private ItemWriter<User> technologyItemWriter;
    
    public UserClassifier(ItemWriter<User> operationsItemWriter, ItemWriter<User> accountsItemWriter, ItemWriter<User> technologyItemWriter) {
//        this.evenItemWriter = evenItemWriter;
//        this.oddItemWriter = oddItemWriter;
    	this.operationsItemWriter = operationsItemWriter;
    	this.accountsItemWriter = accountsItemWriter;
    	this.technologyItemWriter = technologyItemWriter;
    }
 
    @Override
    public ItemWriter<? super User> classify(User user) {
//        return user.getId() % 2 == 0 ? evenItemWriter : oddItemWriter;
    	
    	if(user.getDept().equalsIgnoreCase("operations"))
    		return operationsItemWriter;
    	else if (user.getDept().equalsIgnoreCase("accounts"))
    		return accountsItemWriter;
    	else return technologyItemWriter;
    }
}