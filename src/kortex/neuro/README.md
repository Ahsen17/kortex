# Description for neuro module

## 实体设计

Entity: 主体承载，主要是人、事件或其他（记录元数据）
Fragment：内容片段，记载人、事件或其他的相关内容片段，片段内容避免相似度过高重复
Relationship：记录实体之间的关联关系
Tag：标签，用于实体分类及检索
Series：时序序列，用于记录 Fragment 的时间顺序，以及用于后续的记忆遗忘机制

## 数据存储

以上实体的相关数据存储于关系数据库中，并通过Neo4j图数据库记录关联关系
Fragment 的向量数据存储在向量数据库中，用于相似度检索
Tags 用于实体分类及标签快速检索
Relaitionship 用于图关系构建及检索

## 数据检索

向量相似度、图关系检索、标签检索
检索结果聚合，得出综合评分最高的一部分数据，作为最相似的历史记忆内容
时序关系检索，构建事件先后检索及记忆遗忘机制

## 记忆遗忘

详细设计见 notion 文档

## 技术框架

关系数据库：PostgreSQL
向量数据库：Qdrant
图数据库：Neo4j
时序数据库：PostgreSQL + 分区表

实现语言：Python3.12
Web框架：FastAPI
Agent框架：Agno
数据库框架：
    - sqlalchemy + advanced_alchemy
    - neo4j-driver
    - qdrant-client
    - postgresql + asyncpg
    - valkey 【缓存数据 + 消息队列】
后台任务：
    - worker 【自实现】
    - broker 【自实现】
