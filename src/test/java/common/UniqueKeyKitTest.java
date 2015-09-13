package common;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import ssy.kafka.common.UniqueKeyKit;

/**
 * author: huangqian
 * date  : 15/9/11
 * time  : 下午4:19
 */
@RunWith(BlockJUnit4ClassRunner.class)
public class UniqueKeyKitTest {

    @Test
    public void testUUID(){
        System.out.println(UniqueKeyKit.newUUID());
    }
}
