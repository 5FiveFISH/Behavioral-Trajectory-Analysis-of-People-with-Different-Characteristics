-----------------------------------------------------------------------------------------------------------------------------------------

-- sql1 = '选取模型1和模型4外呼的成交用户'
drop table if exists tmp_call_transaction_cust;
create table tmp_call_transaction_cust as
select 
     cust_id
    ,label
    ,create_time
    ,vocation_mark
    ,travel_sign_amount
from dw.ol_dm_destination_predict_effect
where dt between '20230706' and '20230906' and type in (1, 4) 
    and order_mark_14=1 and travel_sign_amount is not null    -- 外呼后14天内成交
    and cust_id in (select distinct cust_id from dw.kn1_usr_cust_attribute);    -- 剔除已注销用户


-----------------------------------------------------------------------------------------------------------------------------------------

-- sql2 = '包含成交用户近两月的所有行为轨迹'
drop table if exists tmp_cust_access_path_t1;
create table tmp_cust_access_path_t1 as
select 
     cust_id                -- 会员ID
    ,label                  -- 命中标签 1-命中 0-非命中
    ,create_time            -- 下单时间
    ,vocation_mark          -- 度假标识 1-度假产品 0-单资源产品
    ,travel_sign_amount     -- 签约金额
    ,visit_date             -- 访问时间
    ,case when visit_date between date_sub(to_date(create_time), 6) and to_date(create_time) then 1 
        else 0 
    end browsing_flag       -- 访问时间在下单时间近7日内 1-是 0-否
    ,no_missing             -- 访问数据是否缺失（ord不是从1开始） 1-不缺失 0-缺失
    ,access_path            -- 访问路径
from tmp_call_transaction_cust a 
left join (
    -- 获取访问路径
    select 
         vt_mapuid 
        ,to_date(operate_time) visit_date
        ,case when min(ord)=1 then 1 else 0 end no_missing  -- 数据是否缺失 1-不缺失 0-缺失
        ,array(concat_ws('->', collect_list(page_level_1))) access_path
    from (
        select
            vt_mapuid
            ,operate_time
            ,ord
            ,page_level_1
            ,sum(change_group) over (partition by vt_mapuid order by operate_time) group_id -- 按ord分组，并赋值group_id=1、2、3、...
        from (
            select
                cast(vt_mapuid as bigint) vt_mapuid
                ,operate_time
                ,ord
                ,case when page_level_1 like '首页=10.61.0' then '首页'
                    when page_level_1 like 'APP未匹配页面' or page_level_1 like '小程序未匹配页面' then '未匹配页面'
                    when page_level_1 like '新版度假产品详情页' then '度假产品详情页'
                    when page_level_1 like '新版预订流程' then '预订流程'
                    when page_level_1 like '老版对比列表' or page_level_1 like '新版对比列表' then '对比列表'
                    else page_level_1
                end page_level_1    -- 重构路径
                ,case when ord <= lag(ord, 1, ord) over (partition by vt_mapuid order by operate_time) then 1
                    else 0
                end change_group    -- 按ord分组
            from dw.kn1_traf_app_day_detail
            where dt between '20230630' and '20230906' and to_date(operate_time) between '2023-06-30' and '2023-09-06' 
            and vt_mapuid in (select distinct cust_id from tmp_call_transaction_cust)
        ) ranked_data
    ) grouped_data
    group by vt_mapuid, to_date(operate_time), group_id
) b on b.vt_mapuid = a.cust_id;


-----------------------------------------------------------------------------------------------------------------------------------------

-- sql3 = '只保留无行为轨迹的用户和有行为轨迹用户成交近7日内的行为轨迹，并剔除了缺失数据'
drop table if exists tmp_cust_access_path;
create table tmp_cust_access_path as
select 
     cust_id                -- 会员ID
    ,label                  -- 命中标签 1-命中 0-非命中
    ,create_time            -- 下单时间
    ,vocation_mark          -- 度假标识 1-度假产品 0-单资源产品
    ,travel_sign_amount     -- 签约金额
    ,visit_date             -- 访问时间
    ,browsing_flag          -- 访问时间在下单时间近7日内 1-是 0-否
    ,no_missing             -- 访问数据是否缺失（ord不是从1开始） 1-不缺失 0-缺失
    ,access_path            -- 访问路径
from (
    select 
         *
        ,case when max(browsing_flag) over(partition by cust_id) = 1 then 1 else 0 end flag 
    from tmp_cust_access_path_t1
) t
where (flag=0 and (no_missing is null or no_missing=1)) or (flag=1 and browsing_flag=1 and no_missing=1);


-----------------------------------------------------------------------------------------------------------------------------------------
