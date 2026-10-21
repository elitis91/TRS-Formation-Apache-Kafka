package com.transactis.kstreamintegration.basiqueStreams;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

// A vérif
// import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;


import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

public class Business {
	
	public static final String INPUT_TOPIC = "orders";
	public static final String OUTPUT_TOPIC = "ca";
	
	public static void main(String[] args) {
		Properties props = new Properties();
		
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, Uuid.randomUuid().toString());
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2000);
		//props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
		
		final StreamsBuilder builder = new StreamsBuilder();
		
		final ObjectMapper mapper = new ObjectMapper();
		
		KStream<String, String> orders = builder.stream(INPUT_TOPIC);
		
		KTable<String, Double> chiffre_affaire = 
				orders.mapValues(value -> {
					try {
						JsonNode json = mapper.readTree(value);
						return json.get("total_price").asDouble();
					} catch (Exception e) {
						// TODO: handle exception
						System.out.println("Message invalide :" +value);
						return 0.0;
					}
				}).selectKey((key,value) -> "CA_TOTAL" ).groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
				.aggregate(
						() -> 0.0, 
						(key,newValue, total) -> total + newValue, Materialized.with(Serdes.String(), Serdes.Double())
								
						);
		
		chiffre_affaire.toStream().mapValues(total -> "{\"chiffre_affaire\":" +total+"}")
		.to(OUTPUT_TOPIC,Produced.with(Serdes.String(), Serdes.String()));
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		
		streams.start();
	}
}
