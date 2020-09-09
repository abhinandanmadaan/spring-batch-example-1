package com.techprimers.springbatchexample1.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import com.techprimers.springbatchexample1.batch.DBWriter;
import com.techprimers.springbatchexample1.batch.Processor;
import com.techprimers.springbatchexample1.batch.Processor2;
//import com.javacodingskills.spring.batch.demo5.job.JdbcCursorItemReader;
//import com.javacodingskills.spring.batch.demo5.mapper.EmployeeDBRowMapper;
//import com.javacodingskills.spring.batch.demo5.model.Employee;
import com.techprimers.springbatchexample1.model.User;

import mapper.UserDBRowMapper;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {
	
//	private DataSource dataSource;
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	private DataSource dataSource;
	
	@Autowired
	private DBWriter writer1;
	
	@Autowired
	private Processor processor1;



//        return jobBuilderFactory.get("ETL-Load")
//                .incrementer(new RunIdIncrementer())
//                .start(step1)
////                .next(step2)
//                .build();
//    }
    
//    private Resource outputResource = new FileSystemResource("output/employee_output.csv");

    @Bean
    public FlatFileItemReader<User> itemReader() {

        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource("src/main/resources/users.csv"));
        flatFileItemReader.setName("CSV-Reader");
        flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapper());
        System.out.println("inside file reader 1 !!!!!");
        return flatFileItemReader;
    }

    @Bean
    public LineMapper<User> lineMapper() {

        DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(new String[]{"id", "name", "dept", "salary"});

        BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(User.class);

        defaultLineMapper.setLineTokenizer(lineTokenizer);
        defaultLineMapper.setFieldSetMapper(fieldSetMapper);

        return defaultLineMapper;
    }

	
	
    @Bean
    public Step step1() throws Exception{
    	return stepBuilderFactory.get("step1")
    			.<User,User>chunk(100)
    			.reader(itemReader())
    			.processor(processor1)
    			.writer(writer1)
    			.build();
    }
    
    
    
    //STEP -2 DB TO CSV
    
//    @Bean
//    public DataSource dataSource() {
//        DriverManagerDataSource dataSource = new DriverManagerDataSource();
//        dataSource.setDriverClassName("org.h2.Driver"); 
//    	dataSource.setUrl("jdbc:h2:mem:test");   
//    	dataSource.setUsername("sa");   
//    	dataSource.setPassword("");    
//    	return dataSource;
//    }
    
    	@Bean
    	public JdbcCursorItemReader<User> reader2(){
    		JdbcCursorItemReader<User> cursorItemReader = new JdbcCursorItemReader<>();
    		cursorItemReader.setDataSource(dataSource);
    		cursorItemReader.setSql("select * from user");
    		cursorItemReader.setRowMapper(new UserDBRowMapper());
    		System.out.println(cursorItemReader);
    		System.out.println("step 2 reader !!!!!!!!!!!!");
    		return cursorItemReader; 
    	}
    	
//	    @Bean
//	    public ItemStreamReader<User> userDBReader() {
//	        JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<>();
//	        reader.setDataSource(dataSource);
//	        reader.setSql("select * from user");
//	        reader.setRowMapper(new UserDBRowMapper());
//	        System.out.println("Inside UserDB Reader 2!!!!!!");
//	        return reader;
//	    }
    
    	@Bean 
    	public Processor2 processor2() {
			System.out.println("inside Processor 2!!");	    
    		return new Processor2();
    	}

    	private Resource outputResource = new FileSystemResource("output/users_output.csv");
    	
    	@Bean
    	public FlatFileItemWriter<User> writer2(){
    		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
    		writer.setResource(outputResource);
    	
    		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
    		lineAggregator.setDelimiter(",");
    		
    		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
    		fieldExtractor.setNames(new String[] {"id","dept","name","salary","time"});
    		lineAggregator.setFieldExtractor(fieldExtractor);
    		
    		writer.setLineAggregator(lineAggregator);
    		System.out.println("inside Writer 2!!");
    		writer.setShouldDeleteIfExists(true);
    		return writer;
    	}
    	
    	@Bean
        public Step step2() throws Exception{
        	return stepBuilderFactory.get("step2")
        			.<User,User>chunk(100)
        			.reader(reader2())
        			.processor(processor2())
        			.writer(writer2())
        			.build();
        }
    	
    	
    	@Bean
        public Job job() throws Exception{
        	return this.jobBuilderFactory.get("BATCH JOB")
        			.incrementer(new RunIdIncrementer())
        			.start(step1())
        			.next(step2())
        			.build();
        }
    	
}
