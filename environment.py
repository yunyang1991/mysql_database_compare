# -*- coding: utf-8 -*-
__author__ = 'yunyang'

import logging
import logging.handlers
import os
import ConfigParser
import threading
import multiprocessing



class LogLevel:
    """
        日志级别
    """
    ALL = 0
    DEBUG = 10
    INFO = 20
    WARN = 30
    ERROR = 40


class Environment(object):
    # 系统配置文件实例
    _configInstance = None

    # 系统配置文件路径
    _systemConfigFilePath = os.path.dirname(os.path.realpath(__file__)) + "/config.ini"

    # 运行环境文件配置实例
    _runtimeConfigInstance = None


    @staticmethod
    def __getSystemConfigInstance():
        """
        获取配置文件对象
        :return: 配置文件对象
        """
        if Environment._configInstance is None:
            Environment._configInstance = ConfigParser.ConfigParser()
            Environment._configInstance.read(Environment._systemConfigFilePath)

        return Environment._configInstance


    @staticmethod
    def getLogRootDir():
        """
        获取日志文件根目录
        """
        dirInfo = Environment.__getSystemConfigInstance().get("Log", "log_root_dir")

        if dirInfo or dirInfo == "":
            dirInfo = os.path.join(os.path.dirname(os.path.realpath(__file__)), "log")

        dirInfo = dirInfo.replace("\\","/")

        return dirInfo

    @staticmethod
    def getLogLevel():
        """
        default is info
        """
        level = Environment.__getSystemConfigInstance().get("Log", "level")
        level = str.lower(level)
        if level == "all":
            return LogLevel.ALL

        if level == "info":
            return LogLevel.INFO

        if level == "debug":
            return LogLevel.DEBUG

        if level == "error":
            return LogLevel.ERROR

        return LogLevel.INFO

    @staticmethod
    def getOption(section, option):
        """获取配置文件配置项的值
        :param section:配置节点
        :param option:配置选项
        :return:str 配置项的值
        """
        return Environment.__getSystemConfigInstance().get(section, option)

    @staticmethod
    def setOption(section, option, value):
        """
        设置配置文件选项
        """
        Environment.__getSystemConfigInstance().set(section, option, str(value))
        Environment.__getSystemConfigInstance().write(open(Environment._systemConfigFilePath, "w"))
        Environment._configInstance = None
        return True

class BaseLogging(object):


    _threadSafeDt = {}
    _processSafeDt = {}
    t_lock = threading.RLock()
    p_lock = multiprocessing.RLock()

    def __init__(self, fileName, level=LogLevel.INFO):
        fileName = os.path.abspath(fileName)
        if not os.path.exists(os.path.dirname(fileName)):
            os.makedirs(os.path.dirname(fileName))
        logger = logging.getLogger()

        hdlr = logging.handlers.TimedRotatingFileHandler(fileName, 'midnight', 1)
        # hdlr = logging.FileHandler(fileName)
        # set the log format
        hdlr.suffix = "%Y-%m-%d"  # or anything else that strftime will allow
        format = '%(asctime)s [%(levelname)s] %(message)s'
        formatter = logging.Formatter(format)
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

        logger.setLevel(level)
        self.__log = logger


    def info(self, msg):
        if self.__log.level <= LogLevel.INFO:
            self.__log.info(msg)
            print msg


    def debug(self, msg):
        if self.__log.level <= LogLevel.DEBUG:
            self.__log.debug(msg)
            print msg


    def warn(self, msg):
        if self.__log.level <= LogLevel.WARN:
            self.__log.warn(msg)
            print msg


    def error(self, msg):
        if self.__log.level <= LogLevel.ERROR:
            self.__log.error(msg)
            print msg


    @staticmethod
    def __getLogger(app):
        """
        :param app:
        :return:
        """
        if app in ["app"]:
            return BaseLogging.__getLogWithThreadSafe(app)
        return BaseLogging.__getLogWithProcessSafe(app)

    @staticmethod
    def __getLogWithThreadSafe(app):
        """
        在线程安全情况下，获取APP日志
        :return:None
        """

        BaseLogging.t_lock.acquire()
        try:
            if app not in BaseLogging._threadSafeDt.keys():
                if app in ["app"]:
                    fileName = "{0}.txt".format(app)
                else:
                    fileName = "{0}/{0}.txt".format(app)
                level = Environment.getLogLevel()
                asbPath = os.path.join(Environment.getLogRootDir(), fileName)
                logInstence = BaseLogging(asbPath, level)
                BaseLogging._threadSafeDt[app] = logInstence
            return BaseLogging._threadSafeDt[app]
        finally:
            BaseLogging.t_lock.release()

    @staticmethod
    def __getLogWithProcessSafe(app):
        """
        在线程安全情况下，获取APP日志
        :return:None
        """
        BaseLogging.p_lock.acquire()
        try:
            if app not in BaseLogging._processSafeDt.keys():
                if app in ["app"]:
                    fileName = "{0}.txt".format(app)
                else:
                    fileName = "{0}/{0}.txt".format(app)
                level = Environment.getLogLevel()
                asbPath = os.path.join(Environment.getLogRootDir(), fileName)
                logInstence = BaseLogging(asbPath, level)
                BaseLogging._processSafeDt[app] = logInstence
            return BaseLogging._processSafeDt[app]
        finally:
            BaseLogging.p_lock.release()

    @staticmethod
    def getAppLog():
        return BaseLogging.__getLogger("app")

    @staticmethod
    def getSampleLog():
        return BaseLogging.__getLogger("sample")

    @staticmethod
    def getExtractLog():
        return BaseLogging.__getLogger("extract")

    @staticmethod
    def getUploadStaticLog():
       return BaseLogging.__getLogger("upload_static")

    @staticmethod
    def getStaticReportLog():
        return BaseLogging.__getLogger("static_report")

    @staticmethod
    def getUploadStaticReportLog():
        return BaseLogging.__getLogger("upload_static_report")

    @staticmethod
    def getReStaticLog():
        return BaseLogging.__getLogger("restatic")


    @staticmethod
    def getStaticLog():
        return BaseLogging.__getLogger("static")

    @staticmethod
    def getMonitorLog():
        return BaseLogging.__getLogger("monitor")