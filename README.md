# sync-data
本项目可以作为 canal 同步数据到 elasticsearch 的中间服务，接收并处理 kafka 推送过来的 binlog 数据，最终同步到 elasticsearch 中。

Sync-data 在 MySQL 数据同步到 es 中扮演的角色入下图所示。

![sync-data在数据同步中的角色](README/image-20191208234323452.png)

## 项目环境

- canal 1.1.X
- kafka 2.1.1
- Elastic search 7.0.0

## canal

canal 是阿里开源的一项数据同步中间件，可以通过监听 MySQL binlog 日志，实时同步数据到 es 等目的数据库。github地址：https://github.com/alibaba/canal

canal 已经在国内很多互联网公司使用，是MySQL数据同步的一个优秀解决方案。 canal 的使用一般有两种方式：

1. 把 binlog 数据发布到 kafka/rocketMQ 消息中间件，然后自己订阅消息进行处理；
2. 使用 canal 提供的客户端一站式解决数据同步。

本项目使用的就是第一种方式，而 sync-data 要做的事情就是**封装订阅的 kafka 消息，并转储到 es中**。这种方式相比于使用 canal 客户端更加灵活，并且使用消息队列我们更熟悉，目前使用 canal 同步数据在生产环境经受过检验的，应该大多都是采用第一种方式。

### canal配置

#### canal.properties

```properties
#canal.manager.jdbc.password=121212
canal.id = 1
...
# tcp, kafka, RocketMQ
canal.serverMode = kafka
...
canal.mq.servers = 127.0.0.1:9092
...

```

#### instance.properties

```properties
# enable gtid use true/false
canal.instance.gtidon=false

# username/password
canal.instance.dbUsername=root
canal.instance.dbPassword=root
canal.instance.connectionCharset = UTF-8
# enable druid Decrypt database password
canal.instance.enableDruid=false

# table regex
canal.instance.filter.regex=.*\\..*
# table black regex
canal.instance.filter.black.regex=

# mq config
canal.mq.topic=syncdata
canal.mq.partition=0
```



> 目前 sync-data 会把 canal 推送过来的消息全部映射到 elasticsearch 。