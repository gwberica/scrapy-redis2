# scrapy-redis2
###### 此分布式框架是基于scrapy_redis改版而成的，许多方法还是和scrapy_redis一样的，去重换成了bloomfilter

此框架把调度器单独抽出来，使用的时候需要先把调度器启动

### 使用规则：

1. 下载此项目，里面包含一个调度器（scheduler），一个继承scrapy_redis的scrapy项目Demo
2.   scheduler的配置文件是cfg.py   scrapy项目的配置就是和默认的一样，cfg中有mongo 的配置和redis 的配置，根据自己的需要配置，已经默认配置了三种环境，调度器已经实现了不同的环境初始化不同的数据库配置连接池，scrapy项目等完成一个完后的demo 在实现这一块，scrapy项目的配置全部配置在settings
3. 修改redis的key的配置，配置文件是task_defined.py中，这个文件配置了start_url 的key，以及其他优先级的key ，修改这个文件的时候  需要同时修改项目中的scrapy_redis2和调度器中的scrapy_redis2的文件，两个文件是一致的