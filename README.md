# mysql2hive
###基于mysql表结构转换成hive结构建表语句 

    使用： 
        1，配置相应的dbconf.json文件
            注意:{
                 "db_type": "mysql",
                 "ip_addr": "xxx",//连接host只支持单个配置，后续考虑多连接
                 "db_name": "xx,xxx",//可配置多个中间用逗号隔开即可
                 "db_user": "xx",
                 "db_pwd": "xxx",
                 "output_file": "D://hive-sql"//只需要配置文件根目录即可，不需要配置到具体的文件
               }
               
        2，需要什么字段即可在json文件中添加即可 同时需要DbConf struct 中添加相关字段
        
        3，只是一个简单的工具类，需要使用即可将目录下文件copy到您的项目中 或者 git clone xxx到本地配置 run即可。
