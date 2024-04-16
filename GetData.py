import numpy as np
import pandas as pd 

class getData:
  def __init__(self, tag_df, inst_id, db):
    self.tag_df = tag_df
    self.inst_id = inst_id
    self.db = db 

  def extract_data(self):
    """
    DC 실시간 데이터(현 시점으로부터 직전 7일 구간) 가져오기 
    """
    end_times = []
    df_list = []

    for _, row in self.tag_df.iterrows():
      conn, cursor = self.db.sql_connect_1()
      query = f"""SELECT * FROM TABLE 
        WHERE TAG_ID = '{row.TAG_ID}' 
        AND TIME_STAMP > DATEADD(DAY, -7, CURRENT_TIMESTAMP)"""
      data = pd.read_sql(sqpl=query, con = conn)
      conn.close()

      # time_stamp 시작, 끝이 tag별로 조금씩 다르다
      end_time = data.loc[data.last_valid_index(), 'TIME_STAMP']
      end_times.append(end_time)

      data = data.rename(columns = {TAG_VALUE: f"{row.TAG_DESC}"})
      df1 = (
        data[['TIME_STAMP',  f"{row.TAG_DESC}"]]
        .drop_duplicates(subset=['TIME_STAMP'], keep='last')
        .set_index('TIME_STAMP')
      )
      df_list.append(df1)
      df1 = pd.DataFrame()

    return end_times, df_list 
    
def preprocessing(self):
  """
  raw data를 1초 간격 보간, 
  최근 3일 기간만 cutting 후 모든 tag data를 결합 
  """
  scope_hr = 72 * 2 # 3일 + 2시간
  end_times, df_list = self.extract_data()
  end_ = np.min(end_times)
  start_ = end_ - pd.TimeDelta(hours=scope_hr)
  dataset = pd.DataFrame()

  for df in df_list:
    df_ = (
      df
      .resample('1S').ffill()
      .loc[start_:end_]
    )
    dataset = pd.concat([dataset, df_], axis=1)

  if dataset.isnull().sum().sum():
    raise Exception('{',
                    f'"inst_id" : "{self.inst_id}", \
                    "Result": "Fail, Dataset contains NaN. The period is shorter than 74 hours",\
                    "output": -1',
                    '}')
  return dataset 
  
def longest_period_above_mean(self, data):
  """평균을 초과하는 값의 가장 긴 연속 기간"""
  mean_val = data.mean()
  above_mean = data > mean_val
  # 연속된 값의 길이 계산
  consecutive_lengths = (
    above_mean
    .groupby((above_mean!= above_mean.shift()).cumsum())
    .apply(lambda x: x[x].size)
  )
  longest_period = consecutive_lengths.max()
  return longest_perio

def feature_eng(self, data):
  try:
    dff = pd.DataFrame()
    data.columns = [col.replace(' ', '').replace("'", '') for col in data.columns]
    try:
      data['온도차'] = data['온도2'] - data['온도1']
      data['1단비'] = data['토출압력1'] / data['흡입압력1']
      data['2단비'] = data['토출압력2'] / data['토출압력1']
    except KeyError: pass

    mean_df = data.resample('1H', origin='start').mean()
    mean_df.columns = mean_df.columns +'_AVG'
    std_df = data.resample('1H', origin='start').std()
    std_df.columns = std_df.columns +'_STD' 
    min_df = data.resample('1H', origin='start').min()
    min_df.columns = min_df.columns +'_MIN'
    max_df = data.resample('1H', origin='start').max()
    max_df.columns = max_df.columns +'_MAX'   
    meanOverlen_df = (data.
                     resample('1H', origin='start)
                     .apply(self.longest_period_above_mean))
    meanOverlen_df.columns = meanOverlen_df.columns + '_MOLEN'
    df = pd.concat([mean_df, std_df, min_df, max_df, meanOverlen_df], axis=1)

    # 차분 column 추가 
    df_diff = df.diff(2)
    df_diff.columns = df_diff.columns + '(d)'
    dff = pd.concat([df, df_diff],axis=1).iloc[3:]

    dff['압력곱1'] = dff['흡입압력1_AVG']*dff['토출압력1_AVG']
    dff['압력곱2'] = dff['토출압력1_AVG']*dff['토출압력2_AVG']
  except Exception as e:
    print('{',
          f'"inst_id" : "{self.inst_id}", "Result": "Fail, {e}", "output": -1',
          '}')

  return dff












      
