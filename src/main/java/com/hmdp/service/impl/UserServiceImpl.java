package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author tian
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号:利用util下RegexUtils进行正则验证
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式不正确!");
        }
        //2.生成验证码:导入hutool依赖，内有RandomUtil
        String code = RandomUtil.randomNumbers(6);
        //3.保存验证码到redis中
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL , TimeUnit.MINUTES);
        //4.发送验证码
        log.debug("发送验证码成功，验证码：{}",code);

        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1.校验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误!");
        }
        //2.校验验证码
        // 从redis中获取验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if(code==null || !cacheCode.equals(code)){
            //3.不一致，报错
            return Result.fail("验证码错误!");
        }
        //4.一致，根据手机号查询用户(需要写对应的单表查询方法:select * from tb_user where phone = #{phone})
        User user = query().eq("phone", phone).one();
        if(user==null){
            user = creatUserWithPhone(phone);
        }
        //6.存入redis
        // 随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        // 将User对象转为HashMap存储，存储，返回token
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //用BeanUtil将User对象转为Map
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO
                ,new HashMap<>()
                ,CopyOptions.create().ignoreNullValue().setFieldValueEditor(
                        (fieldName,fieldValue) -> fieldValue.toString()
                )
        );
        stringRedisTemplate.opsForHash().putAll(LOGIN_USER_KEY + token,userMap);
        //设置token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL,TimeUnit.MINUTES);
        //返回
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now = LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth - 1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 获取用户签到的数据
        Long userId = UserHolder.getUser().getId();
        LocalDateTime now = LocalDateTime.now();
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        // 拼接key
        String key = USER_SIGN_KEY + userId + keySuffix;
        int day = now.getDayOfMonth();

        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create().
                        get(BitFieldSubCommands.BitFieldType.unsigned(day)).
                        valueAt(0));

        if (CollectionUtil.isEmpty(result)){
            return Result.ok();
        }

        Long num = result.get(0);
        if(num == null || num == 0){
            return Result.ok();
        }

        int count = 0;
        while (true){
            if((num & 1) == 0){
                break;
            }else{
                count++;
            }
            num >>>= 1;
        }


        return Result.ok(count);
    }

    private User creatUserWithPhone(String phone) {
        //创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        //保存用户
        save(user);
        return null;
    }
}
