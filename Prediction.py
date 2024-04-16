import numpy as np
import pandas as pd
import joblib
from scipy.stats import gmean
from config import Constants
import warnings
warnings.filterwarnings('ignore')

class Pred:
  def __init__(self, code, test):
    self.C = constants()
    self.code = code
    self.test = test 

  def reg_ensemble(self, models):
    """
    회귀모델 앙상블(평균) 결과 출력
    """
    pred_df = pd.DataFrame()
    for _, model in models.items():
      pred = model.predict(self.test)
      min_val = np.min(np.abs(pred))
      max_val = np.max(np.abs(pred))
      scaled_pred = (np.abs(pred) - min_val) / (max_val - min_val)
      r_pred = 1 - scaled_pred
      pred_df = pd.concat([pred_df, pd.Series(r_pred)], axis=1)
    m_pred = pred_df.mean(axis=1)

    return m_pred
    
  def predict_failure(self):
    """
    code별로 해당되는 모데을 불러와 test dataset에 대한 failure 예측 수행
    returns: 예측 결과 tuple 형식 반환 
    """
    if self.code == self.C.DC_CODES[0]:
      with open(self.C.MODEL_PATH[0], 'rb') as f:
        models = joblib.load(f)
      pred = models.predict_proba(self.test)
      pred_ = np.where(pred[:,1] > self.C.TH[0], 1, 0)
      
    elif self.code == self.C.DC_CODES[1]:
      with open(self.C.MODEL_PATH[1], 'rb') as f:
        models = joblib.load(f)
      m_pred = self.reg_ensemble(models)
      pred_ = np.where(m_pred > self.C.TH[1], 1, 0)
      
    elif self.code == self.C.DC_CODES[2]:
      with open(self.C.MODEL_PATH[2], 'rb') as f:
        models = joblib.load(f)
      pred_df = pd.DataFrame()
      for _, model in models.items():
        pred = model.predict_proba(self.test)
        pred_df = pd.concat([pred_df, pd.Series(pred[:, 1)], axis=1)

      g_mean = pred_df.apply(lambda row: gmean(row), axis=1)
      pred_ = np.where(g_mean > self.C.TH[2], 1, 0)

    elif self.code == self.C.DC_CODES[3]:
      with open(self.C.MODEL_PATH[3], 'rb') as f:
        models = joblib.load(f)
      pred = models.predict_proba(self.test)
      pred_ = np.where(pred[:,1] > self.C.TH[3], 1, 0)

    elif self.code == self.C.DC_CODES[4]:
      with open(self.C.MODEL_PATH[4], 'rb') as f:
        models = joblib.load(f)
      d_cols = [col for co in self.test.columns if col.startswith('Oil')]
      self.test = sefl.test.drop(columns = d_cols)
      pred = models.predict(self.test)
      pred_ = pred
      
    elif self.code == self.C.DC_CODES[5]:
      with open(self.C.MODEL_PATH[5], 'rb') as f:
        models = joblib.load(f)
      d_cols = [col for co in self.test.columns if col.startswith('Oil')]
      self.test = sefl.test.drop(columns = d_cols)
      m_pred = models.reg_ensemble(models)
      pred_ = np.where(m_pred > self.C.TH[5], 1, 0)

    elif self.code == self.C.DC_CODES[6]:
      with open(self.C.MODEL_PATH[6], 'rb') as f:
        models = joblib.load(f)
      m_pred = models.reg_ensemble(models)
      pred_ = np.where(m_pred > self.C.TH[6], 1, 0)
      
    elif self.code == self.C.DC_CODES[7]:
      with open(self.C.MODEL_PATH[7], 'rb') as f:
        models = joblib.load(f)
      pred_df = pd.DataFrame()
      for _, model in models.items():
        pred = model.predict(self.test)
        scaled_pred = pred + np.min(pred)*(-1)
        min_val = np.min(scaled_pred)
        max_val = np.max(scaled_pred)
        r_pred = (scaled_pred - min_val) / (max_val - min_val)
        pred_df = pd.concat([pred_df, pd.Series(r_pred)], axis=1)

      g_mean = pred_df.apply(lambda row: gmean(row), axis=1)
      pred_ = np.where(g_mean > self.C.TH[7], 1, 0)

    results = pd.DataFrame({'CODE': self.code, 
                            'TIME_STAMP': self.test.reset_index()['TIME_STAMP'], 
                           'PRED': pd.Series(pred_)})
    result_tuples = [tuple(row) for row in results.values]

    return result_tuples 














