package com.zekelabs.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoCallback implements Callback{

	@Override
	public void onCompletion(RecordMetadata arg0, Exception e) {
		// TODO Auto-generated method stub
		System.out.println("Sent successfuly");
		if (e != null) {
			e.printStackTrace();
			}
			}
		
	}
