import argparse
import json
import sys
import pandas as pd
from config import Constants
from GetData import getData
from DBcomm import FetchDB
from Prediction import Pred

# define argparse
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", requred=True, help="JSON input file")
args =parser.parse_args()

dbobj = FetchDB()
C = Constants()

def tag_info(db):
  conn, cursor = db.sql_connect_1()
  query = "SELECT * FROM TABLE"
  data = pd.read_sql(sql=query, con=conn)
  conn.close()
  tags_df = pd.DataFrame(data)
  return tags_df

def run(code, inst_id, db):
  dc_codes = C.DC_CODES
  tags_df = tag_info(db)
  if code in dc_codes:
    c1 = tags_df['CODE'] = code
    c2 = tags_df['TAG_TYPE'] ==0
    tag_df = tags_df.loc[c1&c2]  # 해당 code에 해당하는 tag정보 
  else:
    print('{',
          f'"inst_id" : "{inst_id}", "Result": "Fail, Not valid CODE.", "output": -1',
          '}')
    sys.exit("Force Quit")
    
  # 예측 대상 dataset 구축 
  gd = getData(tag_df, inst_id, db)
  dataset = gd.preprocessing()
  test = gd.feature_eng(dataset)
  
  #예측값 도출
  prd = Pred(code, test)
  result_tuples = prd.predict_failure()
  
  output = db.insert_data(result_tuples)
  if output == 2:
    print('{',
          f'"inst_id" : "{inst_id}", "Result": "Sucess.", "output": 2',
          '}')
  else:
    print('{',
          f'"inst_id" : "{inst_id}", "Result": "Fail, DB Insertion Error.", "output": -1',
          '}')    

def main(input_data, db):
  if 'DPR' in list(input_data.keys()):
    input_cond = input_data['DPR']
    for cond in input_cond:
      inst_id = cond['inst_id']
      param = cond['parameter']
      cod = str(param['code'])
      run(code, inst_id, db)
  else:
    raise Execption('{',
                    f'"inst_id" : " ", \
                      "Result": "Fail, DPR key value does not exist in input_file",\
                      "output": -1',
                      '}')

if __name__=='__main__':
  with open(args.input_file, 'r') as f:
    try: input_data = json.load(f)
    except json.JSONDecodeError:
      raise Exception('{',
          f'"inst_id" : " ", "Result": "Fail, Not a valid JSON string", "output": -1',
          '}')   
  main(input_data, dbobj)
  









