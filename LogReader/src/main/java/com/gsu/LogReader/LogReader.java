package com.gsu.LogReader;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import kafka.serializer.StringEncoder;

public class LogReader implements Runnable {
	private Thread t;
	private String threadName;
	private String bootstrap;
	private String groupid;
	private String topic;
	static long timeout;
	static long count;
	static int illegalLoginCount;
	public static Map<String, SuccessfullLogEvent> customerLoginMap = new HashMap<String, SuccessfullLogEvent>();

	LogReader(String name, String bootstrap, String group, String topic) {
		threadName = name;
		this.bootstrap = bootstrap;
		this.groupid = group;
		this.topic = topic;

	}

	public static Properties get_props(String bootstrap, String groupid) {
		Properties props = new Properties();
		props.put("bootstrap.servers", bootstrap);
		props.put("group.id", groupid);
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "6000");
		props.put("serializer.class", StringEncoder.class.getName());
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("fetch.min.bytes", "50000");
		props.put("receive.buffer.bytes", "262144");
		props.put("fetch.min.bytes", "50000");
		props.put("max.partition.fetch.bytes", "2097152");
		props.put("auto.commit.interval.ms", "1000");
		props.put("autocommit.enable", "true");
		props.put("auto.offset.reset", "earliest");
		props.put("request.timeout.ms", "6100");
		return props;
	}

	public static void main(String[] args) {
		System.out.println(Calendar.getInstance().getTime());

		
		ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
		exec.scheduleAtFixedRate(new Runnable() {
		  @Override
		  public void run() {
			  LogReader consumer0 = new LogReader("Consumer-0", args[0], args[1], args[2]);
			  consumer0.start();
		  }
		}, 0, Long.parseLong(args[3]), TimeUnit.SECONDS);
		
		

	}

	public void start() {

		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}

	@SuppressWarnings({ "resource" })
	@Override
	public void run() {
		KafkaConsumer<String, String> consumer;
		consumer = new KafkaConsumer<>(get_props(bootstrap, groupid));
		consumer.subscribe(Arrays.asList(topic));

		// Document doc = new Document();
		// doc.append("Message", "");
		while (true) {

			ConsumerRecords<String, String> records = consumer.poll(600);
			timeout++;
			if (records.count() > 0)
				timeout = 0;
			if (timeout > 20) {
				System.out.println("Total Suspicious Logins : " + illegalLoginCount);
				System.exit(0);
			}

			for (ConsumerRecord<String, String> record : records) {
				count++;

				String str = record.value().trim();

				int customerIdIndex = str.indexOf("employeeId");
				int customerIdIndexEnd = str.indexOf('"', customerIdIndex + 14);
				String customerId = str.substring(customerIdIndex + 13, customerIdIndexEnd);

				int eventIdIndex = str.indexOf("id");
				int eventIdIndexEnd = str.indexOf('"', eventIdIndex + 5);
				String eventId = str.substring(eventIdIndex + 5, eventIdIndexEnd);

				int latIdIndex = str.indexOf("lat");
				int latIdIndexEnd = str.indexOf(',', latIdIndex + 4);
				String latValue = str.substring(latIdIndex + 5, latIdIndexEnd);

				int lonIdIndex = str.indexOf("long");
				int lonIdIndexEnd = str.indexOf(',', lonIdIndex + 5);
				String lonValue = str.substring(lonIdIndex + 6, lonIdIndexEnd);

				int timeIdIndex = str.indexOf("time");
				String timeValue = str.substring(timeIdIndex + 7, str.length() - 2);

				if (customerLoginMap.containsKey(customerId)) {
					SuccessfullLogEvent current = customerLoginMap.get(customerId);
					if (current.getLat().equalsIgnoreCase(latValue) && current.getLon().equalsIgnoreCase(lonValue)) {
						Date date = null;
						try {
							date = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
									.parse(timeValue.replaceAll("Z$", "+0000"));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						current.setLoginTime(date);
						customerLoginMap.put(customerId, current);
					} else {
						Double factor = calc(Double.parseDouble(latValue), Double.parseDouble(lonValue),
								Double.parseDouble(current.getLat()), Double.parseDouble(current.getLon()));

						Date Newdate = null;
						try {
							Newdate = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
									.parse(timeValue.replaceAll("Z$", "+0000"));
						} catch (ParseException e) {
							e.printStackTrace();
						}
						Date oldDate = current.getLoginTime();

						long millis = Newdate.getTime() - oldDate.getTime();
						Double hours = millis / (1000.0 * 60 * 24);

						if (factor / hours > 60.0) {
							System.out.println(eventId);
							illegalLoginCount++;
						} else {
							SuccessfullLogEvent obj = new SuccessfullLogEvent();
							obj.setLat(latValue);
							obj.setLon(lonValue);

							Date date = null;
							try {
								date = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
										.parse(timeValue.replaceAll("Z$", "+0000"));
							} catch (ParseException e) {
								e.printStackTrace();
							}
							obj.setLoginTime(date);

							customerLoginMap.put(customerId, obj);
						}

					}

				} else {
					SuccessfullLogEvent obj = new SuccessfullLogEvent();
					obj.setLat(latValue);
					obj.setLon(lonValue);

					Date date = null;
					try {
						date = (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
								.parse(timeValue.replaceAll("Z$", "+0000"));
					} catch (ParseException e) {
						e.printStackTrace();
					}
					obj.setLoginTime(date);

					customerLoginMap.put(customerId, obj);
				}

				// System.out.println(customerId + " " + latValue + " " +
				// lonValue + " " + timeValue);

			}
		}

	}

	public static double calc(Double lat1, Double lon1, Double lat2, Double lon2) {

		final double R = 3958.761;
		Double latDistance = toRad(lat2 - lat1);
		Double lonDistance = toRad(lon2 - lon1);
		Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
				+ Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
		Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
		Double distance = R * c;

		return distance;
	}

	private static Double toRad(Double value) {
		return value * Math.PI / 180;
	}

}
