package sea.kafka;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import sea.BigDataApplication;

@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@SpringBootTest(classes = BigDataApplication.class)
public class KafkaTest {
	
	@Resource
	KafkaTemplate kafkaTemplate;
	
	
	@Test
	public void sendMsg(){
		kafkaTemplate.send(KafkaConsumerConfig.topic, "test");
		kafkaTemplate.metrics();
	}

}
