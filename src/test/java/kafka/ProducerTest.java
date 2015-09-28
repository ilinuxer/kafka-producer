package kafka;

import kafka.javaapi.message.MessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.MessageSetSend;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 上午10:42
 */
public class ProducerTest {

    @Test
    public void connectProducer(){
        Properties props = new Properties();
        props.put("metadata.broker.list", "127.0.0.1:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "0");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String,String> producer = new Producer<String, String>(config);
        String topic = "dailyTopTest";
        KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,"hello,daily top");
        producer.send(data);


//        List<KeyedMessage<String,String>> list = new ArrayList<KeyedMessage<String,String>>();
//        for(int i = 0 ; i < 10 ; i++){
//            KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,"111111"+i);
//            list.add(data);
//        }
//        producer.send(list);
        producer.close();

    }


}
