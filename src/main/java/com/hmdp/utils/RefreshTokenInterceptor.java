package com.hmdp.utils;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author tian
 * @description 拦截器
 */
public class RefreshTokenInterceptor implements HandlerInterceptor {
    // 拦截器中将部分数据存储的UserHolder中
    private StringRedisTemplate stringRedisTemplate;
    //因为当前拦截器并没有交给spring控制，所以需要提供构造器，不能通过注解注入
    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        获取获取请求头中的token
        String token = request.getHeader("authorization");
        //System.out.println("token: "+token);

        if (StrUtil.isBlank(token)) {
            return true;
        }
//        HttpSession session = request.getSession();
//        基于token来获取redis中的用户
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
//        Object user = session.getAttribute("user");
//        判断用户是否存在
        if (userMap.isEmpty()) {
            return true;
        }
//        将查询到的hash数据转为hashDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
//        存在，保存用户信息到threadLocal
        UserHolder.saveUser(userDTO);
//        刷新token有效期
        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
//        放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
//        移除用户
        UserHolder.removeUser();
    }
}
