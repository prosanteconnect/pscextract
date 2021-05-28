package fr.ans.psc.pscextract;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication
@EnableMongoRepositories
public class PscextractApplication {

	public static void main(String[] args) {
		SpringApplication.run(PscextractApplication.class, args);
	}

}
