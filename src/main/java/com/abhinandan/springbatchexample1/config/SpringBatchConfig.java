package com.abhinandan.springbatchexample1.config;

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
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.abhinandan.springbatchexample1.batch.DBWriter;
import com.abhinandan.springbatchexample1.batch.Processor;
import com.abhinandan.springbatchexample1.batch.Processor2;
import com.abhinandan.springbatchexample1.classifier.UserClassifier;
import com.abhinandan.springbatchexample1.mapper.UserDBRowMapper;
import com.abhinandan.springbatchexample1.model.User;

import org.springframework.batch.item.ItemStreamReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

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

	
	private Resource outputResource = new FileSystemResource("output/users_output.csv");	//output file of step 2
//	private Resource outputResourceOdd = new FileSystemResource("output/users_odd.csv");
//	private Resource outputResourceEven = new FileSystemResource("output/users_even.csv");
	private Resource outputResourceOp = new FileSystemResource("output/operations_output.csv");	//output file of step 3
	private Resource outputResourceAcc = new FileSystemResource("output/accounts_output.csv");	//output file of step 3
	private Resource outputResourceTech = new FileSystemResource("output/technology_output.csv"); //output file of step 3


	
	//Step 1 - CSV to DB
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
    public Step step1() throws Exception{	// Step 1 - Read CSV and Write to DB
    	return stepBuilderFactory.get("step1")
    			.<User,User>chunk(100)
    			.reader(itemReader())
    			.processor(processor1)
    			.writer(writer1)
    			.build();
    }
    
    
    
    //STEP - 2 DB TO CSV
    
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


	
	@Bean
	public FlatFileItemWriter<User> writer2(){
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
		writer.setResource(outputResource);
	
		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
		fieldExtractor.setNames(new String[] {"id","name","dept","salary","time"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		System.out.println("inside Writer 2!!");
		writer.setShouldDeleteIfExists(true);
		return writer;
	}
    	
	@Bean
    public Step step2() throws Exception{	// Step 2 - Read DB and Write to CSV
    	return stepBuilderFactory.get("step2")
    			.<User,User>chunk(100)
    			.reader(reader2())
    			.processor(processor2())
    			.writer(writer2())
    			.build();
    }
    	
    	
    // Step 3 - CSV to multiple CSV files
    	
	@Bean
    public FlatFileItemReader<User> itemReaderStep3() {

        FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource("output/users_output.csv"));
        flatFileItemReader.setName("CSV-Reader-Step-3");
//      flatFileItemReader.setLinesToSkip(1);
        flatFileItemReader.setLineMapper(lineMapperStep3());
        System.out.println("inside file reader 3 !!!!!");
        return flatFileItemReader;
    }
	
    @Bean
    public LineMapper<User> lineMapperStep3() {

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
	
	
//	@Bean
//	public FlatFileItemWriter<User> evenStep3Writer(){
//		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
//		writer.setResource(outputResourceEven);
//	
//		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
//		lineAggregator.setDelimiter(",");
//		
//		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
//		fieldExtractor.setNames(new String[] {"id","dept","name","salary","time"});
//		lineAggregator.setFieldExtractor(fieldExtractor);
//		
//		writer.setLineAggregator(lineAggregator);
//		System.out.println("inside Even Step3 Writer !!");
//		writer.setShouldDeleteIfExists(true);
//		return writer;
//	}
//	
//	@Bean
//	public FlatFileItemWriter<User> oddStep3Writer(){
//		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
//		writer.setResource(outputResourceOdd);
//	
//		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
//		lineAggregator.setDelimiter(",");
//		
//		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
//		fieldExtractor.setNames(new String[] {"id","dept","name","salary","time"});
//		lineAggregator.setFieldExtractor(fieldExtractor);
//		
//		writer.setLineAggregator(lineAggregator);
//		System.out.println("inside Odd Step3 Writer !!");
//		writer.setShouldDeleteIfExists(true);
//		return writer;
//	}
    	
    @Bean
	public FlatFileItemWriter<User> operationsStep3Writer(){
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
		writer.setResource(outputResourceOp);
	
		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
		fieldExtractor.setNames(new String[] {"id","name","salary","time"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		System.out.println("inside Operations Step3 Writer !!");
		writer.setShouldDeleteIfExists(true);
		return writer;
	}
    
    @Bean
	public FlatFileItemWriter<User> accountsStep3Writer(){
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
		writer.setResource(outputResourceAcc);
	
		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
		fieldExtractor.setNames(new String[] {"id","name","salary","time"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		System.out.println("inside Accounts Step3 Writer !!");
		writer.setShouldDeleteIfExists(true);
		return writer;
	}
    
    @Bean
	public FlatFileItemWriter<User> technologyStep3Writer(){
		FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
		writer.setResource(outputResourceTech);
	
		DelimitedLineAggregator<User> lineAggregator = new DelimitedLineAggregator<User>();
		lineAggregator.setDelimiter(",");
		
		BeanWrapperFieldExtractor<User> fieldExtractor = new BeanWrapperFieldExtractor<User>();
		fieldExtractor.setNames(new String[] {"id","name","salary","time"});
		lineAggregator.setFieldExtractor(fieldExtractor);
		
		writer.setLineAggregator(lineAggregator);
		System.out.println("inside Technology Step3 Writer !!");
		writer.setShouldDeleteIfExists(true);
		return writer;
	}
	
	@Bean
    public ClassifierCompositeItemWriter<User> classifierUserCompositeItemWriter() throws Exception {
        ClassifierCompositeItemWriter<User> compositeItemWriter = new ClassifierCompositeItemWriter<>();
        compositeItemWriter.setClassifier(new UserClassifier(operationsStep3Writer(), accountsStep3Writer(), technologyStep3Writer()));
        return compositeItemWriter;
    }
	
	@Bean
    public Step step3() throws Exception {	// Step 3 - Read CSV produced in Step 2 and Split the data into 3 CSV files based on Deptt
        return stepBuilderFactory.get("step3")
                .<User, User>chunk(10)
                .reader(itemReaderStep3())
                .writer(classifierUserCompositeItemWriter())
                .stream(operationsStep3Writer())
                .stream(accountsStep3Writer())
                .stream(technologyStep3Writer())
                .build();
    }
	
	
	@Bean
    public Job job() throws Exception{
    	return this.jobBuilderFactory.get("BATCH JOB")
    			.incrementer(new RunIdIncrementer())
    			.start(step1())
    			.next(step2())
    			.next(step3())
    			.build();
    }
	
}
