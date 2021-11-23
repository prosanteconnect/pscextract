package fr.ans.psc.pscextract;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

@SpringBootApplication
public class PscextractApplication {

	@Value("${spring.mail.host}")
	private String spring_mail_host;
	@Value("${spring.mail.port}")
	private Integer spring_mail_port;
	@Value("${spring.mail.username}")
	private String spring_mail_username;
	@Value("${spring.mail.password}")
	private String spring_mail_password;

	@Bean
	public JavaMailSender getJavaMailSender() {
		JavaMailSenderImpl mailSender = new JavaMailSenderImpl();
		mailSender.setHost(spring_mail_host);
		mailSender.setPort(spring_mail_port);

		mailSender.setUsername(spring_mail_username);
		mailSender.setPassword(spring_mail_password);

		Properties props = mailSender.getJavaMailProperties();
		props.put("mail.transport.protocol", "smtp");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.debug", "true");

		return mailSender;
	}

	public static void main(String[] args) {
		SpringApplication.run(PscextractApplication.class, args);
	}

}
