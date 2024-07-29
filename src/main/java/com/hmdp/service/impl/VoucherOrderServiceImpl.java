package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author tian
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private IVoucherOrderService iVoucherOrderService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    //注入脚本
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();
    private IVoucherOrderService proxy;
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    private class VoucherOrderHandler implements Runnable{
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中的消息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 MKSTREAM stream.order
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 获取消息是否成功
                    // 失败进行下一次循环
                    if(list == null || list.isEmpty()){
                        continue;
                    }
                    // 解析订单,处理订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // 成功后ack确认
                    stringRedisTemplate.opsForStream()
                            .acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }
            }
        }
        private void handlePendingList() {
            while (true) {
                try {
                    // 获取pending消息队列中的消息 XREADGROUP GROUP g1 c1 COUNT 1  MKSTREAM stream.order
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 获取消息是否成功
                    // 失败进行下一次循环
                    if(list == null || list.isEmpty()){
                        break;
                    }
                    // 解析订单,处理订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // 成功后ack确认
                    stringRedisTemplate.opsForStream()
                            .acknowledge(queueName,"g1",record.getId());
                } catch (Exception e) {
                    log.error("pending-list处理订单异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }
    /*    private BlockingQueue<VoucherOrder> orderTask = new ArrayBlockingQueue<>(1024*1024);
        private class VoucherOrderHandler implements Runnable{
            @Override
            public void run() {
                try {
                    VoucherOrder voucherOrder = orderTask.take();
                    handlerVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常",e);
                }
            }
        }*/
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean isLock = lock.tryLock();

        if(!isLock){
            log.error("不允许重复下单");
            return;
        }

        // 获取代理对象,这里是避免事务失效
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    // 用户购买优惠券
    @Override
    public Result seckillVoucher(Long voucherId) {
        System.err.println(666);
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            // 不为0,代表没有购买资格
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }

        // 创建代理对象,方便用户在别的线程中使用该对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回订单
        return Result.ok(orderId);
    }

 /*   // 用户购买优惠券
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString()
        );

        // 判断结果是否为0
        int r = result.intValue();
        if(r != 0){
            // 不为0,代表没有购买资格
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }

        long orderId = redisIdWorker.nextId("order");
        // 为0,有购买资格
        VoucherOrder voucherOrder = new VoucherOrder();
        // 向订单表中添加数据
        voucherOrder.setId(orderId);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);
        // 创建阻塞队列,并添加
        // 把下单信息保存到阻塞队列
        orderTask.add(voucherOrder);
        // 创建代理对象,方便用户在别的线程中使用该对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 返回订单
        return Result.ok(orderId);
    }*/

/*    @Override
    public Result seckillVoucher(Long voucherId) {
        // 从数据库中获取优惠券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);

        LocalDateTime beginTime = voucher.getBeginTime();
        LocalDateTime endTime = voucher.getEndTime();
        // 判断秒杀是否开始
        // 未开始或时间过期,返回异常结果
        LocalDateTime now = LocalDateTime.now();
        if(now.isBefore(beginTime)){
            return Result.fail("活动还未开始");
        }

        if(now.isAfter(endTime)){
            return Result.fail("活动已过期");
        }
        // 秒杀开始
        // 判断库存是否充足
        // 不充足直接返回
        Integer stock = voucher.getStock();
        if(stock < 1){
            return Result.fail("优惠券已被售空");
        }

        Long userId = UserHolder.getUser().getId();
        //SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
        // 这里改用redisson提供的可从入锁
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        boolean isLock = lock.tryLock();

        if(!isLock){
            return Result.fail("一个用户只能下一单");
        }

        // 获取代理对象,这里是避免事务失效
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

    }*/

    @Transactional
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 同一个用户购买才要加锁,不同用户不需要,内部具有乐观锁来解决
        // 每次用户进来都是新的锁,同一个用户也是,所有这里采用字符串的intern()方法
        // 锁加在这会出现事务未完成,锁就释放的问题!!!!!!!!
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 查询订单,判断是否一个人是否买了同一种优惠券,实现一人一单
        // 查询的时候不方便加乐观锁
        int count = query().eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        if(count > 0){
            log.error("用户已经购买过一次了");
            return;
        }
        // 充足,扣减库存,这里的ge()采用了乐观锁,
        // 原先 eq("stock",voucher.getStock),失败率太高
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock",0)
                .update();
        if(!success){
            log.error("库存不足");
            return;
        }

/*        // 创建订单,返回订单id
        long order = redisIdWorker.nextId(SECKILL_STOCK_KEY);
        VoucherOrder voucherOrder = new VoucherOrder();
        // 向订单表中添加数据
        voucherOrder.setId(order);
        voucherOrder.setVoucherId(voucherId);
        voucherOrder.setUserId(userId);*/
        iVoucherOrderService.save(voucherOrder);
//        return Result.ok(order);

    }
}

