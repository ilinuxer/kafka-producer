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
public class AsyncProducer extends KafkaProducer<String,String> {

    private static final Logger LOG = Logger.getLogger(AsyncProducer.class);

    private int queueBufferingMaxMessages   = QUEUE_BUFFERING_MAX_MESSAGES_100; //异步模式下，producer端允许buffer缓存的消息最大消息数量。
    private long queueBufferingMaxMs        = QUEUE_BUFFERING_MAX_MS_5000;      //异步模式下，缓冲数据的最大时间。
    private long queueEnqueueTimeoutMs      = QUEUE_ENQUEUE_TIMEOUT_MS;         //异步模式下，控制producer沉积阻塞超时时间。
    private int batchNumMessages            = BATCH_NUM_MESSAGES_100;           //异步模式下，每个batch发送的消息数量

    public AsyncProducer(String brokerList,int queueBufferingMaxMessages, long queueBufferingMaxMs,
                         long queueEnqueueTimeoutMs, int batchNumMessages) {
        this.brokerList = brokerList;
        this.queueBufferingMaxMessages = queueBufferingMaxMessages;
        this.queueBufferingMaxMs = queueBufferingMaxMs;
        this.queueEnqueueTimeoutMs = queueEnqueueTimeoutMs;
        this.batchNumMessages = batchNumMessages;
    }

    public static AsyncProducer newInstanceAndOpen(String brokerList,int queueBufferingMaxMessages, long queueBufferingMaxMs,
                                                   long queueEnqueueTimeoutMs, int batchNumMessages){
        AsyncProducer asyncProducer = new AsyncProducer(brokerList,queueBufferingMaxMessages,
                queueBufferingMaxMs,queueEnqueueTimeoutMs,batchNumMessages);
        asyncProducer.open();
        return asyncProducer;
    }

    @Override
    public void setBrokerList(String brokerList) {
        if(StringUtils.isBlank(brokerList)){
            throw new IllegalArgumentException("brokerList can't empty,please set brokeList");
        }
        this.brokerList = brokerList;
    }

    /***
     * 设置异步模式下，producer端允许buffer缓存的消息最大消息数量。
     * 如果消息条数未达到此配置值，producer端将会阻塞。直到达到阀值才会发送。
     * 默认值：queueBufferingMaxMessages＝5000
     * @param queueBufferingMaxMessages producer端允许buffer缓存的消息最大消息数量
     */
    public void setQueueBufferingMaxMessages(int queueBufferingMaxMessages){
        this.queueBufferingMaxMessages = queueBufferingMaxMessages;
    }

    /***
     * 设置异步模式下，缓冲数据的最大时间。
     * 如果设置queueBufferingMaxMs=100，会每隔100ms把所有的消息批量发送。
     * 这会提高吞吐量，但是会增加消息的到达延时。
     * 默认值：queueBufferingMaxMs=100
     * @param queueBufferingMaxMs 缓冲数据的最大时间（单位：ms）
     */
    public void setQueueBufferingMaxMs(long queueBufferingMaxMs){
        this.queueBufferingMaxMs = queueBufferingMaxMs;
    }

    /***
     * 设置阻塞超时时间。（建议为默认值－1）
     * 当消息在producer端沉积的条数达到queue.buffering.max.meesages时，
     * 阻塞一定时间后，队列仍然没有enqueue(producer仍然没有发送出任何消息) 。
     * 此时producer可以继续阻塞或者将消息抛弃，此timeout值用于控制阻塞的时间，如果值为-1则无阻塞超时限制，消息不会被抛弃；
     * 如果值为0则立即清空队列，消息被抛弃。
     * 默认值：queueEnqueueTimeoutMs ＝ －1
     * @param queueEnqueueTimeoutMs producer端沉积阻塞超时时间。
     */
    public void setQueueEnqueueTimeoutMs(long queueEnqueueTimeoutMs){
        this.queueEnqueueTimeoutMs = queueEnqueueTimeoutMs;
    }

    /***
     * 设置异步模式下，一个batch发送的消息数量。
     * producer会等待直到要发送的消息数量达到这个值，之后才会发送。
     * 但如果消息数量不够，达到queue.buffer.max.ms时也会直接发送。
     * 默认值：100
     * @param batchNumMessages 一个batch发送的消息数量
     */
    public void setBatchNumMessages(int batchNumMessages) {
        this.batchNumMessages = batchNumMessages;
    }

    @Override
    protected void setProp(){
        this.prop.setProperty("metadata.broker.list",brokerList);
        this.prop.setProperty("request.required.acks",REQUEST_REQUIRED_ACKS);
        this.prop.setProperty("request.timeout.ms",REQUEST_TIMEOUT_MS);
        this.prop.setProperty("producer.type",ProducerType.ASYNC);
        this.prop.setProperty("serializer.class","kafka.serializer.StringEncoder");
        this.prop.setProperty("partitioner.class",PARTITIONER_CLASS_DEFAULT);
        this.prop.setProperty("compression.codec",CompressionCodec.NONE);
        this.prop.setProperty("message.send.max.retries",MESSAGE_SEND_MAX_RETRIES_10);
        this.prop.setProperty("retry.backoff.ms",RETRY_BACKOFF_MS_100);
        this.prop.setProperty("topic.metadata.refresh.interval.ms",String.valueOf(TOPIC_METADATA_REFRESH_INTERVAL_MS));
        this.prop.setProperty("queue.buffering.max.ms",String.valueOf(queueBufferingMaxMs));
        this.prop.setProperty("queue.buffering.max.messages",String.valueOf(queueBufferingMaxMessages));
        this.prop.setProperty("queue.enqueue.timeout.ms",String.valueOf(queueEnqueueTimeoutMs));
        this.prop.setProperty("batch.num.messages",String.valueOf(batchNumMessages));
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
            LOG.error("send single message error",e);
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
            LOG.error("send message list error",e);
        }
    }
}
