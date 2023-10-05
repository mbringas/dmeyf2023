python3 1_create_quantile_variables.py
cp  competencia_02_quantile_features.csv.gz ~/buckets/b1/datasets/.
python3 2_create_lag_variables.py
mv competencia_02_historical_features.csv.gz ~/buckets/b1/datasets/.
