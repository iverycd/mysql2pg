import configparser
import os
import sys

# 读取配置文件
# 以下在pycharm，主程序调用正常，pyinstall -F生成的可执行文件读取config文件失败
# exepath = os.path.dirname(os.path.abspath(__file__))
# config = configparser.ConfigParser()
# config.read(os.path.join(exepath, 'config.ini'))

exepath = os.getcwd()
exepath = os.path.join(exepath, "config.ini")

config = configparser.ConfigParser()
config.read(exepath)


class ReadConfig:
    def get_mysql(self, name):
        value = config.get('mysql', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value

    def get_postgresql(self, name):
        value = config.get('postgresql', name)  # 通过config.get拿到配置文件中DATABASE的name的对应值
        return value


if __name__ == '__main__':
    print('path值为：', exepath)  # 测试path内容
    print('config_path', exepath + "/config.ini")  # 打印输出config_path测试内容是否正确
    print('通过config.get拿到配置文件中DATABASE的mysql host的对应值:',
          ReadConfig().get_mysql('host'))  # 通过上面的ReadConfig().get_mysql方法获取配置文件中DATABASE的'host'的对应值为10.182.27.158
