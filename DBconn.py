import pymssql
import pandas as pd
from config import Constants

class FetchDB:
  def __init__(self):
    self.C = Constants()
    self.serverip = self.C.SERVERS
    self.user = self.C.USERS
    self.password = self.C.PASSWORD
    self.database = self.C.DATABASE

  def sql_connect_1(self):
    conn = pymssql.connect(
      server = self.serverip[0], user = self.user[0], password = self.password, database = self.database[0]
    )
    cursor = conn.cursor()
    return conn, cursor

  def sql_connect_2(self):
    conn = pymssql.connect(
      server = self.serverip[1], user = self.user[1], password = self.password, database = self.database[1]
    )
    cursor = conn.cursor()
    return conn, cursor

  def insert_data(self, results):
    """
    예측 결과를 database에 입력
    동일한 시간대가 db에 있으면 UPDATE, 없으면 INSERT
    return: 1 (insert 성공) / 0 ( insert 실패 )
    """
    conn, cursor = self.sql_connect_2()
    table = "TABLE_NAME"
    try:
    	for record in result_tuples:
        code = record[0]
        pred_time = record[1]
        pred_val = record[2]

        query_check = f"""
          SELECT * FROM {table} 
          WHERE DATEADD(HOUR, DATEDIFF(HOUR, 0, PRED_TIME), 0) = %s 
          AND CODE = %s
          """
        cursor.execute(query_check, (pred_time.replace(minute = 0, secode =0), code)
        existing_record = cursor.fetchone()
        if existing_record:
          query_update = f"""UPDATE {table} 
            SET PRED_TIME = %s, PRED_VALUE=%s, TXN_TIME = CURRENT_TIMESTAMP
            WHERE CODE = %s AND DATEADD(HOUR, DATEDIFF(HOUR, 0, PRED_TIME), 0)=%s
            """
          cursor.execute(query_update, (pred_time, pred_val, code, pred_time.replace(minute=0, second=0))
          conn.commit()
        else:
          cols = "CODE, PRED_TIME, PRED_VALUE, TXN_TIME"
          query = f"INSERT INTO {table} ({cols}) VALUES (%s, %s, %s, CURRENT_TIMESTAMP)"
          cursor.execute(query, record)

      output_param = 2
    except Exception as e:
      output_param = -1
    
    conn.close()
    return output_param
