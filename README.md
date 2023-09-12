# <font color = 'green' >外呼14天内成交用户的行为轨迹分析</font>
<br>

## <font color = 'blue' >一、项目描述</font>

### **1. 背景描述**  
&emsp;&emsp;机器人外呼是某旅游平台为了维护用户，提高用户留存率和转化率的一项日常业务运营工作。为了提高用户维护效率，降低维护工作成本，需要有针对性地筛选最有维护价值的目标人群进行维护，实现效率和价值最大化。  
&emsp;&emsp;根据不同算法模型筛选出目标用户群体，对这部分人群进行机器人外呼，得到外呼命中与否的结果，结果保存在数据库中。外呼命中是指通过智能外呼得到用户有出行或平台下单意向的真实反馈。  

### **2. 项目内容**
&emsp;&emsp;该项目选取近两个月内在机器人外呼14天内下单且成交的用户群体，基于用户的流量数据，提取该部分人群的行为轨迹数据，分析不同特征群体的行为轨迹特征，以优化平台页面展示和产品推荐，提高用户转化率。  
&emsp;&emsp;**主要工作步骤如下：**  
1. **数据库取数：** 导出外呼14天内成交用户群体名单，以及该群体在订单成交前历史7天内的平台访问路径；  
2. **数据分析：** 针对不同特征群体绘制桑基图，分析用户在成交前的行为轨迹特征；
3. **结论与建议：** 总结数据分析结果。

<br>

## <font color = 'blue' >二、导数</font>
### **1. 外呼成交用户相关信息** 
&emsp;&emsp;选取模型1和模型4外呼的成交用户，包括用户ID、命中标签、下单时间、下单产品类型、签约金额，数据保存在表`tmp_call_transaction_cust`。
``` sql
-- 选取模型1和模型4外呼的成交用户
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
```
### **2. 用户行为轨迹**
&emsp;&emsp;首先，根据上述成交用户名单，查询该群体的近两月内所有的行为轨迹，对浏览时间和缺失数据进行标注，结果保存在表`tmp_cust_access_path_t1`。
&emsp;&emsp;缺失数据指用户访问会话（session）的顺序（ord）不是从1开始。
``` sql
-- 包含成交用户近两月的所有行为轨迹
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
from tmp_cwq_call_transaction_cust a 
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
            and vt_mapuid in (select distinct cust_id from tmp_cwq_call_transaction_cust)
        ) ranked_data
    ) grouped_data
    group by vt_mapuid, to_date(operate_time), group_id
) b on b.vt_mapuid = a.cust_id;
```

&emsp;&emsp;最后，对上表进行筛选，只保留无行为轨迹的用户和有行为轨迹用户成交前近7日内的行为轨迹，并且，剔除了缺失数据，提取出了15130位会员的139566条行为轨迹数据，结果保存在表`tmp_cust_access_path`。
``` sql
-- 只保留无行为轨迹的用户和有行为轨迹用户成交近7日内的行为轨迹，并剔除了缺失数据
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
```
 
<br>

## <font color = 'blue' >三、行为轨迹分析</font>
``` python
'''数据获取'''
import subprocess as sbs
sbs.run('source /etc/profile; hadoop fs -getmerge hdfs://emr-cluster/user/****/warehouse/tmp_cust_access_path /opt/****/jupyter/****/tmp_data/tmp_cust_access_path.csv', shell=True)

import pandas as pd 
colnames = ['cust_id', 'label', 'create_time', 'vocation_mark', 'travel_sign_amount', 'visit_date', 'browsing_flag', 'no_missing', 'access_path']
data = pd.read_csv("/opt/****/jupyter/****/tmp_data/tmp_cust_access_path.csv", encoding='utf-8', sep=chr(1), names=colnames, na_values='\\N')
data


# 数据去重
drop_duplicates_data = data.drop_duplicates(subset=['cust_id', 'create_time'])
drop_duplicates_data

  
'''订单成交前命中情况'''
import plotly.express as px
counts = drop_duplicates_data['label'].value_counts()
fig = px.pie(counts, values=counts.values, names=counts.index.map({1:'命中',0:'非命中'}), hole=0.3,
             color_discrete_sequence=px.colors.sequential.Teal)
fig.update_traces(textinfo='percent+label+value', pull=[0.1]+[0] * len(counts), rotation=90)
fig.update_layout(width=400, height=400, showlegend=False)
fig.show()

  
'''成交产品分布'''
import plotly.express as px
counts = drop_duplicates_data['vocation_mark'].value_counts()
fig = px.pie(counts, values=counts.values, names=counts.index.map({1:'度假产品',0:'单资源产品'}), hole=0.3,
             color_discrete_sequence=px.colors.sequential.Teal)
fig.update_traces(textinfo='percent+label+value', pull=[0.1]+[0] * len(counts), rotation=90)
fig.update_layout(width=400, height=400, showlegend=False)
fig.show()
```
&emsp;&emsp;成交订单中，用户命中率为11.1%，度假产品占比15.2%。
<div style="display: flex; justify-content: center; align-items: center;">
  <div style="text-align: center;">
    <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111724001.png" alt="成交订单中命中情况分布" width="400" />
    <p>成交订单中命中情况分布</p>
  </div>
  <div style="text-align: center;">
    <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111725061.png" alt="成交产品类型分布" width="400" />
    <p>成交产品类型分布</p>
  </div>
</div>

``` python
'''成交近7日内有无浏览'''
import plotly.express as px

browsing = data.groupby('cust_id')['browsing_flag'].unique().reset_index()
counts = browsing['browsing_flag'].value_counts()
fig = px.pie(counts, values=counts.values, names=counts.index, hole=0.3,
             color_discrete_sequence=px.colors.sequential.Teal)
fig.update_traces(textinfo='percent+label+value', pull=[0.1]+[0] * len(counts))
fig.update_layout(width=400, height=400, showlegend=False)
fig.show()
```
&emsp;&emsp;成交用户中，在成交前近7日内61.5%的用户进行过浏览。
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111726930.png" alt="" width="400" />
</div>  

&emsp;&emsp;下面对不同用户群体的行为轨迹进行分析，通过绘制桑基图查看用户的行为轨迹。
&emsp;&emsp;桑基图（Sankey diagram）是一种可视化工具，用于展示数据流动、转化或流程的信息。这里被用来可视化用户行为轨迹数据，具体来说，它展示了用户从一个行为到另一个行为之间的转化过程，以及这些转化的频率。
- **节点（Nodes）：**
  &emsp;&emsp;在桑基图中，每个节点代表一个行为或状态。这些节点按照数据中出现的顺序排列，表示用户行为轨迹中的不同步骤或事件。节点标签旁边的括号中显示了节点的频率信息。这表示在用户行为轨迹数据中，该节点出现的次数或频率。
  &emsp;&emsp;当鼠标悬停在节点上时，会显示该节点标签名称、入流量计数（流入该节点的数量 incoming flow count）、出流量计数（流出该节点的数量 outgoing flow count）。下图为例，此处表示节点“单品品类”的出现频率为663，入流量为655，出流量为635，有655-635=20条路径止于该步骤。
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111729448.png" alt="" width="400" />
</div>

- **边（Links）：**
&emsp;&emsp;边表示从一个节点到另一个节点的转化路径。每条边的宽度（或值）代表了从一个节点到另一个节点的转化频率或数量。边的宽度越宽，表示边的流量越大。当鼠标悬停在边上时，会显示该边的流出节点（source）和流入节点（target）。下图为例，此处该边表示从“度假产品详情页”到“通用产品列表”的转化过程。
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111729017.png" alt="" width="400" />
</div>

``` python
import plotly.graph_objects as go
import random


def access_path_sankey(data, save_path, group_name):
    '''
        para:
            data: 用户行为轨迹数据
            save_path: 轨迹图保存路径
            group_name: 群体名称
        return:
            top_5_node_labels: 点击量top5的节点名称
            top_5_node_frequencies: 点击量top5的节点频率
            top_5_node_conversion_rates: 点击量top5的节点转化率
    '''
    # 统计每个链接的出现频率
    link_frequencies = data.str.split('->').explode().value_counts().to_dict()
    # 拆分访问路径并为每个路径元素创建唯一编号
    path_elements = data.str.split('->').explode().unique()
    node_ids = {element: i for i, element in enumerate(path_elements)}
    # 创建节点列表和边列表，并记录每个节点的频率
    nodes = [{'label': element, 'frequency': link_frequencies.get(element, 0)} for element in path_elements]
    edges = []

    # 构建桑葚图的节点和边
    for path in data:
        path_elements = path.split('->')
        for i in range(len(path_elements) - 1):
            source_node = node_ids[path_elements[i]]
            target_node = node_ids[path_elements[i + 1]]
            edges.append((source_node, target_node))

    # 创建不同颜色的节点和边
    node_colors = [f'rgb({random.randint(0, 255)}, {random.randint(0, 255)}, {random.randint(0, 255)})' for _ in nodes]
    edge_colors = [node_colors[source_node] for source_node, _ in edges]

    # 创建桑基图
    fig = go.Figure(go.Sankey(
        node=dict(
            pad=20,
            thickness=30,
            line=dict(color=node_colors, width=0.5),  # 设置节点的颜色
            label=[f"{node['label']} ({node['frequency']})" for node in nodes]
        ),
        link=dict(
            source=[edge[0] for edge in edges],
            target=[edge[1] for edge in edges],
            value=[link_frequencies[nodes[edge[0]]['label']] for edge in edges],  # 设置边的值为链接的频率
            color=edge_colors  # 设置边的颜色与源节点一致
        )
    ))

    # 设置图形布局和标题
    fig.update_layout(title=f"{group_name}行为轨迹图（按访问频率）")
    fig.write_html(fr"{save_path}/{group_name}行为轨迹图.html")
    
    # 找到前5个频率最高的节点
    top_5_nodes = sorted(nodes, key=lambda x: x['frequency'], reverse=True)[:5]
    # 提取前5个节点的名称和频率值
    top_5_node_labels = [node['label'] for node in top_5_nodes]
    top_5_node_frequencies = [node['frequency'] for node in top_5_nodes]
    # 计算前5个节点的转化率
    top_5_node_conversion_rates = []
    for label in top_5_node_labels:
        incoming_links = [edge for edge in edges if nodes[edge[1]]['label'] == label]
        outgoing_links = [edge for edge in edges if nodes[edge[0]]['label'] == label]
        if len(incoming_links) == 0:
            conversion_rate = 0  # 处理入流量为零的情况
        else:
            conversion_rate = len(outgoing_links) / len(incoming_links)
        top_5_node_conversion_rates.append(conversion_rate)

    # 返回前5个节点名称、频率值和转化率
    return top_5_node_labels, top_5_node_frequencies, top_5_node_conversion_rates
```
``` python
'''随机抽取部分数据以绘制轨迹图'''
# 删除access_path列缺失值
data = data.dropna(subset=['access_path'])
# 随机抽取500条数据
data_sampled = data.sample(n=500, random_state=42)
# data_sampled


'''调用access_path_sankey()函数绘制用户行为轨迹的桑基图'''
save_path = ''  ## 添加图片存储路径

# 命中人群
access_path_sankey(data_sampled[data_sampled['label']==1]['access_path'], save_path, '命中人群')
# 非命中人群
access_path_sankey(data_sampled[data_sampled['label']==0]['access_path'], save_path, '非命中人群')
# 度假产品订单人群
access_path_sankey(data_sampled[data_sampled['vocation_mark']==1]['access_path'], save_path, '度假产品订单人群')
# 单资源产品订单人群
access_path_sankey(data_sampled[data_sampled['vocation_mark']==0]['access_path'], save_path, '单资源产品订单人群')
```
&emsp;&emsp;随机抽取500条路径，绘制了不同群体的行为轨迹图。
<div align="center">
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111729493.png" alt="命中人群行为轨迹图" width="1200" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111730048.png" alt="非命中人群行为轨迹图" width="1200" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111730313.png" alt="度假产品订单人群行为轨迹图" width="1200" />
  <img src="https://raw.githubusercontent.com/5FiveFISH/Figure/main/img/202309111730098.png" alt="单资源产品订单人群行为轨迹图" width="1200" />
</div> 
&emsp;&emsp;点击量Top5的节点、节点出现频率，以及该节点的转化率（转化率 = 出流量数 / 入流量数）如下表。

<!-- |                        |     **节点**    | **频率** | **节点转化率** |
|:----------------------:|:---------------:|:--------:|:--------------:|
|      **命中人群**      |     单品品类    |    663   |     96.95%     |
|                        |     会员中心    |    175   |      100%      |
|                        |     预订流程    |    162   |     98.11%     |
|                        |  度假产品详情页 |    97    |     95.79%     |
|                        |       首页      |    86    |     164.71%    |
|     **非命中人群**     |     单品品类    |   2556   |     93.70%     |
|                        |     会员中心    |    687   |     95.55%     |
|                        |     预订流程    |    463   |     94.32%     |
|                        |       首页      |    446   |     177.97%    |
|                        |  度假产品详情页 |    440   |     95.34%     |
|  **度假产品订单人群**  |  度假产品详情页 |    415   |     96.79%     |
|                        |     预定流程    |    308   |     95.02%     |
|                        |     会员中心    |    264   |     98.84%     |
|                        |   通用产品列表  |    179   |     94.89%     |
|                        | Android默认打点 |    154   |     99.35%     |
| **单资源产品订单人群** |     单品品类    |   3139   |     94.48%     |
|                        |     会员中心    |    598   |     95.36%     |
|                        |       首页      |    433   |     175.64%    |
|                        |     预订流程    |    317   |     95.57%     |
|                        | Android默认打点 |    255   |     95.29%     | -->


<!DOCTYPE html>
<html>
<head>
<style>
  table {
    border-collapse: collapse;
    width: 100%;
  }
  th, td {
    border: 1px solid black;
    padding: 8px;
    text-align: center;
  }
  th {
    background-color: #6b8e9c; /* 设置首行背景色为蓝灰色 */
    color: white !important; /* 设置首行文字颜色为白色 */
    /* font-weight: bold; */
  }
  td:first-child {
    font-weight: bold; /* 设置首列文字加粗 */
  }
</style>
</head>
<body>

<div style="text-align: center;">
  <table>
    <tr>
      <th></th>
      <th>节点</th>
      <th>频率</th>
      <th>节点转化率</th>
    </tr>
    <tr>
      <td rowspan="5">命中人群</td>
      <td>单品品类</td>
      <td>663</td>
      <td>96.95%</td>
    </tr>
    <tr>
      <td>会员中心</td>
      <td>175</td>
      <td>100%</td>
    </tr>
    <tr>
      <td>预订流程</td>
      <td>162</td>
      <td>98.11%</td>
    </tr>
    <tr>
      <td>度假产品详情页</td>
      <td>97</td>
      <td>95.79%</td>
    </tr>
    <tr>
      <td>首页</td>
      <td>86</td>
      <td>164.71%</td>
    </tr>
    <tr>
      <td rowspan="5">非命中人群</td>
      <td>单品品类</td>
      <td>2556</td>
      <td>93.70%</td>
    </tr>
    <tr>
      <td>会员中心</td>
      <td>687</td>
      <td>95.55%</td>
    </tr>
    <tr>
      <td>预订流程</td>
      <td>463</td>
      <td>94.32%</td>
    </tr>
    <tr>
      <td>首页</td>
      <td>446</td>
      <td>177.97%</td>
    </tr>
    <tr>
      <td>度假产品详情页</td>
      <td>440</td>
      <td>95.34%</td>
    </tr>
    <tr>
      <td rowspan="5">度假产品订单人群</td>
      <td>度假产品详情页</td>
      <td>415</td>
      <td>96.79%</td>
    </tr>
    <tr>
      <td>预定流程</td>
      <td>308</td>
      <td>95.02%</td>
    </tr>
    <tr>
      <td>会员中心</td>
      <td>264</td>
      <td>98.84%</td>
    </tr>
    <tr>
      <td>通用产品列表</td>
      <td>179</td>
      <td>94.89%</td>
    </tr>
    <tr>
      <td>Android默认打点</td>
      <td>154</td>
      <td>99.35%</td>
    </tr>
    <tr>
      <td rowspan="5">单资源产品订单人群</td>
      <td>单品品类</td>
      <td>3139</td>
      <td>94.48%</td>
    </tr>
    <tr>
      <td>会员中心</td>
      <td>598</td>
      <td>95.36%</td>
    </tr>
    <tr>
      <td>首页</td>
      <td>433</td>
      <td>175.64%</td>
    </tr>
    <tr>
      <td>预订流程</td>
      <td>317</td>
      <td>95.57%</td>
    </tr>
    <tr>
      <td>Android默认打点</td>
      <td>255</td>
      <td>95.29%</td>
    </tr>
  </table>
</div>

</body>
</html>


&emsp;&emsp;成交用户中，命中人群和非命中人群的行为轨迹差异不大，top5的节点一致，点击量最多的都是单品品类。另外，命中人群各节点的转化率均高于非命中人群，首页除外。相比于非命中人群，命中人群“单品品类”的转化率高3.25%，“度假产品详情页”的转化率高0.45%，“预定流程”的转化率高3.79%。
&emsp;&emsp;度假产品成交人群与单资源产品成交人群的行为轨迹存在较大差异，度假产品成交人群的行为轨迹围绕“度假产品详情页”、“预订流程”、“会员中心”、“通用产品列表”、“Android默认打点”展开，“度假产品详情页”的转化率达96.79%，而单资源产品成交人群的行为轨迹则围绕“单品品类”、“会员中心”、“首页”、“预定流程”、“Android默认打点”展开，“单品品类”的转化率达94.48%，且不涉及度假产品相关的浏览。

<br>

&emsp;&emsp;【附：完整轨迹图】
<iframe src="D:/cwq/job/画像分析/命中人群行为轨迹图.html" width="100%" height="400px"></iframe>
<iframe src="D:/cwq/job/画像分析/非命中人群行为轨迹图.html" width="100%" height="400px"></iframe>
<iframe src="D:/cwq/job/画像分析/度假产品订单人群行为轨迹图.html" width="100%" height="400px"></iframe>
<iframe src="D:/cwq/job/画像分析/单资源产品订单人群行为轨迹图.html" width="100%" height="400px"></iframe>
