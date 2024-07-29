package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.aspectj.weaver.ast.Var;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.FOLLOW_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author tian
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private IUserService userService;

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = FOLLOW_KEY + userId;
        if(isFollow){
            // 添加关注
            Follow follow = new Follow();
            follow.setFollowUserId(followUserId);
            follow.setUserId(userId);
            follow.setCreateTime(LocalDateTime.now());
            boolean isSuccess = save(follow);
            if(isSuccess){
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }

        }else{
            // 进行取关
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId)
                    .eq("follow_user_id", followUserId));
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result isFollow(Long followUserId) {
        //1.获取登录用户
        Long userId = UserHolder.getUser().getId();
        //2.查询是否关注
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        //判断
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommon(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key1 = FOLLOW_KEY + userId;
        String key2 = FOLLOW_KEY + id;
        // 取交集
        Set<String> set = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if(CollectionUtil.isEmpty(set)){
            return Result.ok(Collections.emptyList());
        }
        // 装换为UserDto集合
        List<Long> followsId = set.stream().map(Long::valueOf).collect(Collectors.toList());

        List<UserDTO> users = userService.listByIds(followsId).stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());


        return Result.ok(users);
    }
}
