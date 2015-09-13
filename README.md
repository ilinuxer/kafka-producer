#Kafka Producer

## 介绍

kafka-producer是ssy-dmp小组基于Kafka客户端0.8.1.1版本进行开发的一个Kafka生产者应用程序。该版本的客户端和ssy-dmp小组应用的Kafka中间件对应。kafka-producer是为了不了解Kafka的同学提供的工具，提供了同步和异步两种消费者客户端。


## 使用方式

 kafka-producer使用者，需要通过KafkaProducerBuilder去构建一个KafkaProducer实力，使用方式和Guava的Cache非常相似，指定好参数进行构建（关于参数，请参考"KafkaProducerBuilder参数说明"）。
 
 为了保证Kafka消息的伸缩性，kafka-producer中并不提供具体的消息格式，只是指定了消息格式化的接口MessageFormatter，使用者可以自己定制消息的格式。使用者实现MessageFormatter接口，并且实现format方法，在format方法中指定消息的格式。

### 同步例子


	@RunWith(BlockJUnit4ClassRunner.class)
	public class SyncProducerTest {
	
	    private KafkaProducer<String,String> syncProducer = KafkaProducerBuilder.getBuilder()
	            .setSendType(SendType.SYNC)
	            .setBrokerList("192.168.30.102:9092").build();
	
	    @Test
	    public void testCreateSyncProducer(){
	        syncProducer.send("dailyTopTest",new TestMessage());
	        List<TestMessage> lst = new ArrayList<TestMessage>();
	        for(int i = 0; i < 10; i ++){
	            lst.add(new TestMessage());
	        }
	        syncProducer.send("dailyTopTest",lst);
	        syncProducer.close();
	
	    }
	
	    class TestMessage implements MessageFormatter {
	
	        @Override
	        public String format() {
	            return UniqueKeyKit.newUUID()+"|"+System.currentTimeMillis();
	        }
	    }
	
	    @Test
	    public void testCastTypeBuilder(){
	        SyncProducer producer = KafkaProducerBuilder.getBuilder()
	                .setBrokerList("192.168.30.102:9092")
	                .setSendType(SendType.SYNC)
	                .build(SyncProducer.class);
	        producer.send("dailyTopTest",new TestMessage());
	    }
	
	}

	
	
	
### 异步例子

	@RunWith(BlockJUnit4ClassRunner.class)
	public class AsynProducerTest {
	
	    private KafkaProducer<String,String> asyncProducer = KafkaProducerBuilder.getBuilder()
	            .setBrokerList("192.168.30.102:9092")
	            .setSendType(SendType.ASYNC)
	            .setBatchNumMessages(10)
	            .build();
	
	
	    @Test
	    public void testAsyncProducer(){
	        List<MessageFormatter> lst = new ArrayList<MessageFormatter>();
	        for(int i = 0; i < 100; i ++){
	            lst.add(new TestMessage());
	        }
	        asyncProducer.send("dailyTopTest",lst);
	        asyncProducer.close();
	    }
	    class TestMessage implements MessageFormatter {
	
	        @Override
	        public String format() {
	            return UniqueKeyKit.newUUID()+"|"+System.currentTimeMillis();
	        }
	    }
	
	    @Test
	    public void testCastTypeBuilder(){
	        AsyncProducer producer = KafkaProducerBuilder.getBuilder()
	                .setBrokerList("192.168.30.102:9092")
	                .setSendType(SendType.ASYNC)
	                .setBatchNumMessages(2)
	                .build(AsyncProducer.class);
	        List<MessageFormatter> lst = new ArrayList<MessageFormatter>();
	        for(int i = 0; i < 10; i ++){
	            lst.add(new TestMessage());
	        }
	        producer.send("dailyTopTest",lst);
	    }
	}
	
----
	
## KafkaProducerBuilder参数说明

+ brokerList

  设置Kafka的Broker集群访问地址。必须设置。

+ queueBufferingMaxMessages 

	设置异步模式下，producer端允许buffer缓存的消息最大消息数量。 如果消息条数未达到此配置值，producer端将会阻塞。直到达到阀值才会发送。 
	
	+ 默认值：queueBufferingMaxMessages＝5000
	
	
+ queueBufferingMaxMs

	设置异步模式下，缓冲数据的最大时间。如果设置queueBufferingMaxMs=100，会每隔100ms把所有的消息批量发送。这会提高吞吐量，但是会增加消息的到达延时。
	
	+ 默认值：queueBufferingMaxMs=100

+ queueEnqueueTimeoutMs

	设置阻塞超时时间（建议为默认值－1）。当消息在producer端沉积的条数达到queue.buffering.max.meesages时，阻塞一定时间后，队列仍然没有enqueue(producer仍然没有发送出任何消息)，此时producer可以继续阻塞或者将消息抛弃，此timeout值用于控制阻塞的时间，如果值为-1则无阻塞超时限制，消息不会被抛弃；如果值为0则立即清空队列，消息被抛弃。
	
   + 默认值：queueEnqueueTimeoutMs ＝ －1

+ batchNumMessages

 	设置异步模式下，一个batch发送的消息数量。producer会等待直到要发送的消息数量达到这个值，之后才会发送。但如果消息数量不够，达到queue.buffer.max.ms时也会直接发送。
 	
   + 默认值：100

+ sendType

	sendType 发送模式。同步/异步两种
	
	+ 默认值：sendType = SendTYPE.SYNC（同步）

----	

	
## kafka-producer的开发参数列表
 
  以下参数对于kafka-producer使用者不必阅读，如果你是kafka-producer项目维护和开发者，必须阅读。

+ metadata.broker.list 默认值：无，必填

	格式为host1:port1,host2:port2，这是一个broker列表，用于获得元数据(topics，partitions和replicas)，建立起来的socket连接用于发送实际数据，这个列表可以是broker的一个子集，或者一个VIP，指向broker的一个子集。
 
+ request.required.acks 默认值：0

	用来控制一个produce请求怎样才能算完成，准确的说，是有多少broker必须已经提交数据到log文件，并向leader发送ack，可以设置如下的值：
0，意味着producer永远不会等待一个来自broker的ack，这就是0.7版本的行为。这个选项提供了最低的延迟，但是持久化的保证是最弱的，当server挂掉的时候会丢失一些数据。
1，意味着在leader replica已经接收到数据后，producer会得到一个ack。这个选项提供了更好的持久性，因为在server确认请求成功处理后，client才会返回。如果刚写到leader上，还没来得及复制leader就挂了，那么消息才可能会丢失。
-1，意味着在所有的ISR都接收到数据后，producer才得到一个ack。这个选项提供了最好的持久性，只要还有一个replica存活，那么数据就不会丢失。
request.timeout.ms 默认值：10000
请求超时时间。
 
+ producer.type 默认值：sync

	决定消息是否应在一个后台线程异步发送。合法的值为sync，表示异步发送；sync表示同步发送。设置为async则允许批量发送请求，这回带来更高的吞吐量，但是client的机器挂了的话会丢失还没有发送的数据。异步意味着消息将会在本地buffer，并适时批量发送，推荐使用。
 
+ serializer.class 默认值：kafka.serializer.DefaultEncoder

	The serializer class for messages. The default encoder takes a byte[] and returns the same byte[].
消息的序列化类，默认是的encoder处理一个byte[]，返回一个byte[]。
 
+ key.serializer.class 默认值：无

	The serializer class for keys (defaults to the same as for messages if nothing is given).
key的序列化类，默认跟消息的序列化类一样。
 
+ partitioner.class 默认值：kafka.producer.DefaultPartitioner

	用来把消息分到各个partition中，默认行为是对key进行hash。
 
+ compression.codec 默认值：none

	允许指定压缩codec来对消息进行压缩，合法的值为：none，gzip，snappy。
 
+ compressed.topics 默认值：null

	允许你指定特定的topic对其进行压缩。如果compression codec设置了除NoCompressionCodec以外的值，那么仅会对本选项指定的topic进行压缩。如果compression codec为NoCompressionCodec，那么压缩就不会启用。
 
+ message.send.max.retries 默认值：3

	如果producer发送消息失败了会自动重发，本选项指定了重发的次数。注意如果是非0值，那么可能会导致重复发送，就是说的确发送了消息，但是没有收到ack，那么就还会发一次。
 
+ retry.backoff.ms 默认值：100

	在每次重发之前，producer会刷新相关的topic的元数据，来看看是否选出了一个新leader。由于选举leader会花一些时间，此选项指定了在刷新元数据前等待的时间。
 
+ topic.metadata.refresh.interval.ms 默认值：600 * 1000

	当出现错误时(缺失partition，leader不可用等)，producer通常会从broker拉取最新的topic的元数据。它也会每隔一段时间轮询(默认是每隔10分钟)。如果设置了一个负数，那么只有当发生错误时才会刷新元数据，当然不推荐这样做。有一个重要的提示：只有在消息被发送后才会刷新，所以如果producer没有发送一个消息的话，则元数据永远不会被刷新。
 
queue.buffering.max.ms 默认值：5000
当使用异步模式时，缓冲数据的最大时间。例如设为100的话，会每隔100毫秒把所有的消息批量发送。这会提高吞吐量，但是会增加消息的到达延时。
 
+ queue.buffering.max.messages 默认值：10000

	在异步模式下，producer端允许buffer的最大消息数量，如果producer无法尽快将消息发送给broker，从而导致消息在producer端大量沉积，如果消息的条数达到此配置值，将会导致producer端阻塞或者消息被抛弃。
 
+ queue.enqueue.timeout.ms 默认值：-1

	当消息在producer端沉积的条数达到queue.buffering.max.meesages时，阻塞一定时间后，队列仍然没有enqueue(producer仍然没有发送出任何消息) 。此时producer可以继续阻塞或者将消息抛弃，此timeout值用于控制阻塞的时间，如果值为-1则无阻塞超时限制，消息不会被抛弃；如果值为0则立即清空队列，消息被抛弃。
 
+ batch.num.messages 默认值：200

	在异步模式下，一个batch发送的消息数量。producer会等待直到要发送的消息数量达到这个值，之后才会发送。但如果消息数量不够，达到queue.buffer.max.ms时也会直接发送。
 
+ send.buffer.bytes 默认值：100 * 1024

	socket的发送缓存大小。
 
+ client.id 默认值：""

	这是用户可自定义的client id，附加在每一条消息上来帮助跟踪。
