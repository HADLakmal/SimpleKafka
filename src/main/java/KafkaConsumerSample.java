import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Collections;
import java.util.Properties;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.Gson;

class MsgKafka {

    private String id;
    private String timestamp;
    private String data;

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }

}

public class KafkaConsumerSample
{
    public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "127.0.0.2:9092,127.0.0.3:9092,127.0.0.4:9092");
        properties.put("kafka.topic"      , "mos.accounts");
        properties.put("compression.type" , "gzip");
        properties.put("key.deserializer"   , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("max.partition.fetch.bytes", "2097152");
        properties.put("max.poll.records"          , "500");
        properties.put("group.id"          , "damind");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG          , "earliest");

        runMainLoop(args, properties);
    }

    static void runMainLoop(String[] args, Properties properties) throws InterruptedException, UnsupportedEncodingException {

        // Create Kafka producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        try {

            consumer.subscribe(Arrays.asList(properties.getProperty("kafka.topic")));

            System.out.println("Subscribed to topic " + properties.getProperty("kafka.topic"));

            consumer.assignment();

            while (true)
            {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("partition = %s, offset = %d \n", record.partition(), record.offset() );
                    System.out.println(record.key());
                }

            }
        }

        finally {
            consumer.close();
        }
    }

    public static MsgKafka decodeMsg(String json) throws UnsupportedEncodingException {

        Gson gson = new Gson();

        MsgKafka msg = gson.fromJson(json, MsgKafka.class);

        byte[] encodedData = Base64.getDecoder().decode(msg.getData());
        msg.setData(new String(encodedData, "utf-8"));

        return msg;
    }
}
