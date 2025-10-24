from flask import Flask, request, jsonify
import mlflow.sklearn
import pandas as pd

app = Flask(__name__)

# load model artifact path (adjust to your MLflow run/model path)
MODEL_PATH = 'models:/rf_model/1'  # or local path like 'runs:/<run_id>/rf_model'
model = mlflow.sklearn.load_model(MODEL_PATH)

@app.route('/predict', methods=['POST'])
def predict():
    payload = request.get_json()
    # expect payload: {"data": [[avg_amount, txn_count], ...], "columns": [..]}
    data = payload.get('data')
    cols = payload.get('columns', ['avg_amount', 'txn_count'])
    df = pd.DataFrame(data, columns=cols)
    probs = model.predict_proba(df)[:,1]
    results = probs.tolist()
    return jsonify({'predictions': results})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1234)