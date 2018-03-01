import pandas as pd
from sklearn.externals import joblib
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
import statsmodels.api as sm
logreg = LogisticRegression()


data = pd.read_csv("data.csv", sep=";")
data_final = data
print(data_final.columns.values)
data_final_vars = data_final.columns.values.tolist()
y = data_final['Y']
X = [i for i in data_final_vars if i not in 'Y']
X = data_final[X]
# print(y)
# logit_model= sm.Logit(y,X)
# result=logit_model.fit()
# print(result.summary())
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=0)
logreg = LogisticRegression()
logreg.fit(X_train, y_train)
filename = 'finalized_model.sav'
joblib.dump(logreg, filename)
print(X_test)
A = ([[0, 1, 0, 6]])
y_pred = logreg.predict(X_test)
a = logreg.predict_proba(A)
print(A)
print(a[0][0])
print('Accuracy of logistic regression classifier on test set: {:.2f}'.format(logreg.score(X_test, y_test)))

loaded_model = joblib.load(filename)
result = loaded_model.predict_proba(A)
print(result[0][0])