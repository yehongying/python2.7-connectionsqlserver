# -*- encoding:utf-8 -*-
# time: 2016-12-20
# author: wgh
import pymssql
from saveerrorlogs import *

# 数据库操作类


class ConnectionSqlServer:
    strsql = "none"
    # 连接SqlServer字符串
    # user="sa", password="All_View_Consulting_2014@", host="192.168.2.236", timeout=30
    sa = "sa"  # 用户名
    # password = "All_View_Consulting_2014@"  # 密码
    password = "3132_deeposh_0083"  # 密码
    host = "192.168.2.245"  # 数据库ip
    timeout = 120  # 超时时间设置
    database = "sanc"  # 数据库
    charset = "utf8"  # 数据库编码

    def __init__(self, strsql):
        ConnectionSqlServer.strsql = strsql

    # 查询SqlServer数据库并 return 查询结果数组
    def selectsqlserverandreturn(self):
        # print (u"开始连接SqlServer")
        wgh = True
        wa = 1
        while wgh:
            try:
                con = pymssql.connect(user="%s" % self.sa, password="%s" % self.password , host="%s" % self.host,
                                      timeout="%s" % self.timeout, charset="%s" % self.charset, database="%s" % self.database)
            except pymssql.OperationalError, e:
                print (u"连接SqlServer数据库错误：{0}".format(e.message))
                wr = SaveErrorLogsFile("连接SqlServer数据库错误：{0}".format(e.message))
                wr.saveerrorlog()
                # wr = SaveErrorLogsFile("连接SqlServer数据库错误：%s" % e.message)
                # wr.saveerrorlog()
                wgh = True
                wa += 1
                print ("开始连接SqlServer第 %s 次" % wa)
                time.sleep(10)
            else:
                wgh = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.execute(sql)
                except Exception, e:
                    wr = SaveErrorLogsFile("连接SqlServer数据库错误：{0}".format(e.message))
                    wr.saveerrorlog()
                    break
                returnax = []
                for ix in cur:
                    returnax.append(ix)
                cur.close()
                con.commit()
                con.close()
                return returnax

    def inserintosqlserver(self, values):
        wgh = True
        wa = 1
        while wgh:
            try:
                con = pymssql.connect(user="%s" % self.sa, password="%s" % self.password, host="%s" % self.host,
                                      timeout="%s" % self.timeout, charset="%s" % self.charset, database="%s" % self.database)
            except pymssql.OperationalError, e:
                wr = SaveErrorLogsFile("连接SqlServer数据库错误：{0}".format(e))
                wr.saveerrorlog()
                wgh = True
                wa += 1
                print ("开始连接SqlServer第 %s 次" % wa)
                time.sleep(10)
            else:
                wgh = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.executemany(sql, values)
                except pymssql.OperationalError, e:
                    print ("插入数据错误:{0}".format(e))
                    if "20004" in str(e):
                        # 20004 表示断网,等待5s重新插入数据
                        print ("SqlServer断网,等待5s重新插入数据")
                        cur.close()
                        con.commit()
                        con.close()
                        time.sleep(5)
                        wgh = True
                    else:
                        wgh = False
                    wr = SaveErrorLogsFile("插入数据错误：{0}".format(e))
                    wr.saveerrorlog()
                    con.rollback()
                    cur.close()
                    con.close()
                else:
                    cur.close()
                    con.commit()
                    con.close()

    def executesqlserver(self):
        wgh = True
        wa = 1
        while wgh:
            try:
                con = pymssql.connect(user="%s" % self.sa, password="%s" % self.password, host="%s" % self.host,
                                      timeout="%s" % self.timeout, charset="%s" % self.charset, database="%s" % self.database)
            except pymssql.OperationalError, e:
                wr = SaveErrorLogsFile("连接SqlServer数据库错误：{0}".format(e.message))
                wr.saveerrorlog()
                wgh = True
                wa += 1
                print ("开始连接SqlServer第 %s 次" % wa)
                time.sleep(10)
            else:
                wgh = False
                cur = con.cursor()
                sql = self.strsql
                try:
                    cur.execute(sql)
                except pymssql.OperationalError, e:
                    wgh = False
                    wr = SaveErrorLogsFile("执行execute错误信息：{0}".format(e.message))
                    wr.saveerrorlog()
                    con.rollback()
                    cur.close()
                    con.close()
                    print e.message
                else:
                    cur.close()
                    con.commit()
                    con.close()
                    print ("成功创建表,结束,关闭SqlServer")


