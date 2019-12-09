# sync-data
本项目可以作为 canal 同步数据到 elasticsearch 的中间服务，接收并处理 kafka 推送过来的 binlog 数据，最终同步到 elasticsearch 中。

Sync-data 在 MySQL 数据同步到 es 中扮演的角色入下图所示。

![sync-data在数据同步中的角色](README/image-20191208234323452.png)

## canal

canal 是阿里开源的一项数据同步中间件，可以通过监听 MySQL binlog 日志，实时同步数据到 es 等目的数据库。github地址：https://github.com/alibaba/canal

canal 已经在国内很多互联网公司使用，是MySQL数据同步的一个优秀解决方案。 canal 的使用一般有两种方式：

1. 把 binlog 数据发布到 kafka/rocketMQ 消息中间件，然后自己订阅消息进行处理；
2. 使用 canal 提供的客户端一站式解决数据同步。

本项目使用的就是第一种方式，而 sync-data 要做的事情就是**封装订阅的 kafka 消息，并转储到 es中**。这种方式相比于使用 canal 客户端更加灵活，并且使用消息队列我们更熟悉，目前使用 canal 同步数据在生产环境经受过检验的，应该大多都是采用第一种方式。









drop

```json
{"data":null,"database":"canal","es":1575862970000,"id":20,"isDdl":true,"mysqlType":null,"old":null,"pkNames":null,"sql":"DROP TABLE `case` /* generated by server */","sqlType":null,"table":"case","ts":1575862970756,"type":"ERASE"}
```

insert

```json
{"data":[{"id":"1","role_name":"系统管理员","station_id":"1"}],"database":"canal","es":1575863063000,"id":21,"isDdl":false,"mysqlType":{"id":"int(11)","role_name":"varchar(10)","station_id":"int(11)"},"old":null,"pkNames":["id"],"sql":"","sqlType":{"id":4,"role_name":12,"station_id":4},"table":"role","ts":1575863063727,"type":"INSERT"}
```







