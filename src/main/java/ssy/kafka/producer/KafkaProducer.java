package ssy.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import ssy.kafka.message.MessageFormatter;

import java.util.List;
import java.util.Properties;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午4:16
 */
public abstract class KafkaProducer<K,V> implements ProducerConstants{

    protected Properties prop   = new Properties();
    protected String brokerList;
    protected ProducerConfig producerConfig;
    protected Producer<K,V> producer;

    public abstract void setBrokerList(String brokerList);

    public abstract void send(String topic,MessageFormatter messageFormater);
    public abstract void send(String topic,List<? extends MessageFormatter> abstractMessageList);

    protected abstract void setProp();

    public abstract void open();

    public abstract void close();


    /***
     * 创建一个Kafka生产者
     */
    public void creatProducer(Properties props){
        producerConfig = new ProducerConfig(props);
        producer = new Producer<K, V>(producerConfig);
    }
}
