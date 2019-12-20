import xgboost as xgb
import pandas as pd
import numpy as np

modelo = xgb.Booster({'nthread': 4})  # init model
modelo.load_model("xgboost.model")  # load data

class Gatos:
    def __init__(self):
        self.client = []
    def prediccion(self, results):
        Bwt = np.array(results.get("Bwt"), dtype = np.float) #body weight in kg.
        Hwt = np.array(results.get("Hwt"), dtype = np.float) #heart weight in g.
        df = pd.DataFrame(data={'Bwt': Bwt, 'Hwt': Hwt})
        print(df)
        dtest = xgb.DMatrix(df)
        probs = modelo.predict(dtest)
        ypred = (probs > 0.38) * 1
        Result = np.where(ypred == 0, "F", "M")
        return(Result.tolist())

#Bwt = np.array([2, 4], dtype = np.float)
#Hwt = np.array([3.8, 15], dtype = np.float)
#df = pd.DataFrame(data = {'Bwt': Bwt, 'Hwt': Hwt})
#print(df)
#modelo = xgb.Booster({'nthread': 4})  # init model
#modelo.load_model("xgboost.model")  # load data
#dtest = xgb.DMatrix(df)
#probs = modelo.predict(dtest)
#ypred = (probs > 0.38)*1
#Result = np.where(ypred == 0,"F","M")
#Result.tolist()
