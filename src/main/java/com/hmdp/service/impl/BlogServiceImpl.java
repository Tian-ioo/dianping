package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author tian
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Integer id) {
        //1.查询blog
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("笔记不存在");
        }
        //2.查询blog有关的用户
        queryBlogUser(blog);
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 判断用户是否点赞
        // 获取用户id
        UserDTO user = UserHolder.getUser();
        if(user == null){
            return;
        }

        String userId = user.getId().toString();
        String key = BLOG_LIKED_KEY + blog.getId();

        // 判断用户是否在点赞的用户set集合中
        Double score = stringRedisTemplate.opsForZSet().score(key, userId);
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
        // 点赞功能
        // 获取用户id
        String userId = UserHolder.getUser().getId().toString();
        String key = BLOG_LIKED_KEY + id;

        // 判断用户是否在点赞的用户set集合中
        Double score = stringRedisTemplate.opsForZSet().score(key, userId);

        if(score == null){
            // 不在,like = like + 1 ,可以点赞
            boolean isSuccess = update().setSql("liked = liked + 1")
                    .eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key,userId,System.currentTimeMillis());
            }
        }else{
            // 在,like = like - 1
            boolean isSuccess = update().setSql("liked = liked - 1")
                    .eq("id", id).update();
            if(isSuccess){
                stringRedisTemplate.opsForZSet().remove(key,userId);
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Integer id) {
        // 查询点赞前五名
        String key = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);

        // redis中数据不存在
        if(CollectionUtil.isEmpty(top5)){
            return Result.ok(Collections.emptyList());
        }
        // Long::parseLong
        // Long::valueOf底层走的是Long::parseLong,但是他会重缓存区-128 ~127找数据
        List<Long> ids = top5.stream()
                .map(Long::valueOf)
                .collect(Collectors.toList());

        String join = StrUtil.join(",", ids);

        List<UserDTO> users = userService.query()
                .in("id",ids).last("ORDER BY FIELD ( id," + join + " )").list()
                .stream().map(user -> BeanUtil.copyProperties(user,UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(users);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        Long userId = user.getId();
        blog.setUserId(userId);
        // 保存探店博文
        boolean isSuccess = save(blog);

        if(!isSuccess){
            return Result.fail("发布笔记失败");
        }
        // 查询发布用户的粉丝id
        List<Follow> follows = followService.query().eq("follow_user_id", userId).list();

        for (Follow follow : follows) {
            Long followUserId = follow.getUserId();
            String key = FEED_KEY + followUserId;
            stringRedisTemplate.opsForZSet()
                    .add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Long offset) {
        // 滚动分页查询,用于用户的收件箱进行查询
        // 获取单前用户id
        Long userId = UserHolder.getUser().getId();

        // 分页拉取
        String key = FEED_KEY + userId;
        // 查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);

        if(CollectionUtil.isEmpty(typedTuples)){
            return Result.ok();
        }

        // 解析数据 blogId minTime(时间戳) offset
        List <Long> ids = new ArrayList<>(typedTuples.size());

        long minTime = 0;
        int os = 1;

        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // 获取id
            ids.add(Long.valueOf(tuple.getValue()));
            // 获取分数
            long time = tuple.getScore().longValue();
            if(minTime == time){
                os++;
            }else{
                minTime = time;
                os = 1;
            }
        }

        String join = StrUtil.join(",", ids);

        List<Blog> blogs = query().in("id", ids)
                .last("ORDER BY FIELD ( id," + join + " )")
                .list();

        for (Blog blog : blogs) {
            // 查询单前博客的观看用户和博客点赞
            queryBlogUser(blog);
            isBlogLiked(blog);
        }


        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(os);

        return Result.ok(r);
    }


    //查询blog相关用户功能
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}


