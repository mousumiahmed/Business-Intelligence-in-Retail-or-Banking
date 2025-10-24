import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score

# For prototype, read features from CSV export or small delta extract
# Example loads a CSV exported from the feature Delta table

df = pd.read_csv('data/labeled_features.csv')  # ensure columns: customer_id, avg_amount, txn_count, label
X = df[['avg_amount', 'txn_count']]
y = df['label']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=50, random_state=42)
    model.fit(X_train, y_train)
    preds = model.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, preds)
    acc = accuracy_score(y_test, model.predict(X_test))
    mlflow.log_metric('auc', auc)
    mlflow.log_metric('accuracy', acc)
    mlflow.sklearn.log_model(model, 'rf_model')
    print('logged model: auc=', auc, 'acc=', acc)