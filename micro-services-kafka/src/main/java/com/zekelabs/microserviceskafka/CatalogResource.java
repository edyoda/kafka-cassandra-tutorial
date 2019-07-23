package com.zekelabs.microserviceskafka;


import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.reactive.function.client.WebClient;

@RestController
@RequestMapping("/catalog")
public class CatalogResource {

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    WebClient.Builder webClientBuilder;
    
    @Autowired
	private KafkaTemplate<Object, Object> template;

	@GetMapping(path = "/send")
	public void sendFoo() {
		this.template.send("topic1", "Great World");
	}
    
//    static void runProducer() {
//		Producer<Long, String> producer = ProducerCreator.createProducer();
//
//		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
//		
//			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
//					"Hello");
//			try {
//				RecordMetadata metadata = producer.send(record).get();
//				//producer.send(record, new DemoCallback());
//				System.out.println(index);
//				System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
//						+ " with offset " + metadata.offset());
//				
//			} catch (ExecutionException e) {
//				System.out.println("Error in sending record");
//				System.out.println(e);
//			} catch (InterruptedException e) {
//				System.out.println("Error in sending record");
//				System.out.println(e);
//			}
//		}
//	}
//
//    @RequestMapping("/{userId}")
//    public String getCatalog(@PathVariable("userId") String userId) {
//
//        /*
//    	UserRating userRating = restTemplate.getForObject("http://ratings-data-service/ratingsdata/user/" + userId, UserRating.class);
//
//        return userRating.getRatings().stream()
//                .map(rating -> {
//                    Movie movie = restTemplate.getForObject("http://movie-info-service/movies/" + rating.getMovieId(), Movie.class);
//                    return new CatalogItem(movie.getName(), movie.getDescription(), rating.getRating());
//                })
//                .collect(Collectors.toList());
//        */
//    	runProducer();
//    	return "Done";
//    }
}

/*
Alternative WebClient way
Movie movie = webClientBuilder.build().get().uri("http://localhost:8082/movies/"+ rating.getMovieId())
.retrieve().bodyToMono(Movie.class).block();
*/