package ssy.kafka.producer;

import kafka.producer.KeyedMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import ssy.kafka.common.Toolkit;
import ssy.kafka.message.MessageFormatter;

import java.util.ArrayList;
import java.util.List;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午4:12
 */
public class SyncProducer extends KafkaProducer<String,String> {

    private static final Logger LOG = Logger.getLogger(SyncProducer.class);

    public SyncProducer(String brokerList){
        this.brokerList = brokerList;
    }

    public static SyncProducer newInstanceAndOpen(String brokerList){
        SyncProducer syncProducer = new SyncProducer(brokerList);
        syncProducer.open();
        return syncProducer;
    }


    @Override
    public void setBrokerList(String brokerList) {
        if(StringUtils.isBlank(brokerList)){
            throw new IllegalArgumentException("brokerList can't empty,please set brokeList");
        }
        this.brokerList = brokerList;
    }

    @Override
    protected void setProp(){
        this.prop.setProperty("metadata.broker.list",brokerList);
        this.prop.setProperty("request.required.acks",REQUEST_REQUIRED_ACKS);
        this.prop.setProperty("request.timeout.ms",REQUEST_TIMEOUT_MS);
        this.prop.setProperty("producer.type", ProducerConstants.ProducerType.SYNC);
        this.prop.setProperty("serializer.class","kafka.serializer.StringEncoder");
        this.prop.setProperty("partitioner.class",PARTITIONER_CLASS_DEFAULT);
        this.prop.setProperty("compression.codec", ProducerConstants.CompressionCodec.NONE);
        this.prop.setProperty("message.send.max.retries",MESSAGE_SEND_MAX_RETRIES_10);
        this.prop.setProperty("retry.backoff.ms",RETRY_BACKOFF_MS_100);
        this.prop.setProperty("topic.metadata.refresh.interval.ms",String.valueOf(TOPIC_METADATA_REFRESH_INTERVAL_MS));
        this.prop.setProperty("send.buffer.bytes",SEND_BUFFER_BYTES_100K);

    }

    @Override
    public void open() {
        Toolkit.ifNull(prop,"can not not found any config ");
        this.setProp();
        this.creatProducer(this.prop);
    }

    @Override
    public void close() {
        if(Toolkit.isNotNull(producer)){
            producer.close();
        }
    }

    @Override
    public void send(String topic,MessageFormatter messageFormater) {
        try {
            KeyedMessage<String,String> message = new KeyedMessage<String, String>(topic, messageFormater.format());
            this.producer.send(message);
        } catch (Exception e) {
            LOG.error("sync send single message error",e);
        }
    }

    @Override
    public void send(String topic, List<? extends MessageFormatter> messagesList) {
        try {
            List<KeyedMessage<String,String>> keyedMessagesList = new ArrayList<KeyedMessage<String,String>>();
            for(MessageFormatter message : messagesList){
                keyedMessagesList.add(new KeyedMessage<String, String>(topic,message.format()));
            }
            this.producer.send(keyedMessagesList);
        } catch (Exception e) {
            LOG.error("sync send message list error",e);
        }
    }
}
