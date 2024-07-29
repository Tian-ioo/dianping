package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;
    @Resource
    private RedisIdWorker redisIdWorker;
    
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }

    @Test
    void test() throws InterruptedException {
        shopService.saveShopToRedis(1L,10L);
    }

    @Test
    void loadShopData() {
        // 查询店铺信息
        List<Shop> list = shopService.list();
        // 对数据进行分组
        Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 导入redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 获取同类型id
            Long shopType = entry.getKey();
            String key = SHOP_GEO_KEY + shopType;
            // 获取同类型集合
            List<Shop> shops = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(shops.size());
            // 每次单条插入数据太慢
/*            for (Shop shop : shops) {
                stringRedisTemplate.opsForGeo()
                        .add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
            }*/
            for (Shop shop : shops) {
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(),shop.getY())
                ));
            }

            stringRedisTemplate.opsForGeo().add(key,locations);

        }
    }

    @Test
    void testHypeLogLog() {

        String [] values = new String[1000];
        int j = 0;

        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_"+i;
            if(j == 999){
                stringRedisTemplate.opsForHyperLogLog().add("hl2",values);
            }
        }

        System.out.println(stringRedisTemplate.opsForHyperLogLog().size("hl2"));
    }

    @Test
    void redisTest(){
        Set<String> set = new HashSet<>();
        for (int i = 0; i < 51; i++) {
            set.add(String.valueOf(i));
        }
        stringRedisTemplate.opsForSet().add("users",set.toArray(new String[0]));
    }
}
