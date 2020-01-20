# scrapy-redis2
此分布式框架是基于scrapy_redis改版而成的，许多方法还是和scrapy_redis一样的，去重换成了bloomfilter
此框架把调度器单独抽出来，使用的时候需要先把调度器启动
使用规则：
  下载此项目，里面包含一个调度器（scheduler），一个继承scrapy_redis的scrapy项目Demo
  scheduler的配置文件是cfg.py   scrapy项目的配置就是和默认的一样，去修改需要改的配置
  rediskey的修改地方是在task_defined.py中，这个文件配置了start_url 的key，以及其他优先级的key 
     修改这个文件的时候  需要同时修改项目中的scrapy_redis2和调度器中的scrapy_redis2的文件，两个文件是一致的
