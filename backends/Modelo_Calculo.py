from sklearn.externals import joblib


class BqClient:
    def __init__(self):
        self.client = []
    def get_score(self, results):
        X1 = int(results.get("X1"))
        X2 = int(results.get("X2"))
        X3 = int(results.get("X3"))
        X4 = int(results.get("X4"))
        response = {}
        loaded_model = joblib.load('finalized_model.sav')
        A = ([[X1, X2, X3, X4]])
        print(A)
        response['score'] = loaded_model.predict_proba(A)[0][0]
        return response




