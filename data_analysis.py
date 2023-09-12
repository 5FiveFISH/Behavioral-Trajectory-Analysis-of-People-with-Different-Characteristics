# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

'''成交近7日内有无浏览'''
import plotly.express as px

browsing = data.groupby('cust_id')['browsing_flag'].unique().reset_index()
counts = browsing['browsing_flag'].value_counts()
fig = px.pie(counts, values=counts.values, names=counts.index, hole=0.3,
             color_discrete_sequence=px.colors.sequential.Teal)
fig.update_traces(textinfo='percent+label+value', pull=[0.1]+[0] * len(counts))
fig.update_layout(width=400, height=400, showlegend=False)
fig.show()


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

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


# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
