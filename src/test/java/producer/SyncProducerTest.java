package producer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import ssy.kafka.common.UniqueKeyKit;
import ssy.kafka.message.MessageFormatter;
import ssy.kafka.producer.*;

import java.util.ArrayList;
import java.util.List;

/**
 * author: huangqian
 * date  : 15/9/10
 * time  : 下午6:32
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class SyncProducerTest {

    private KafkaProducer<String,String> syncProducer = KafkaProducerBuilder.getBuilder()
            .setSendType(SendType.SYNC)
            .setBrokerList("192.168.30.102:9092").build();

    @Test
    public void testCreateSyncProducer(){
        syncProducer.send("news_request_stat",new TestMessage());
        List<TestMessage> lst = new ArrayList<TestMessage>();
        for(int i = 0; i < 10; i ++){
            lst.add(new TestMessage());
        }
        syncProducer.send("news_request_stat",lst);
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
