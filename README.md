# BTC Intelligence X — incremental research release
Replace the four existing files and add intelligence.py. Streamlit entrypoint app.py. Python 3.11 recommended. Run `pip install -r requirements.txt` then `streamlit run app.py` locally.
Live terminal retains V5 feeds and chart analysis and adds a price-level footprint, near-mid book levels, conditional Monte Carlo scenarios and checkpoint logging. Performance lab accepts manually verified outcomes and reports accuracy and Brier score. Keep the live terminal open at checkpoint times; logs are local and may disappear on cloud restart. Not official BRTI. No trained ML, historical order-book heatmap, automatic settlement, futures liquidation feed, actual trades or validated probabilities. Do not use simulated frequencies as betting odds.

X Pro additions: ML lab with logistic regression, histogram gradient boosting and random forest; chronological holdout with horizon purge; candidates screened by Brier score. Six-hour retraining while process runs; not durable scheduling. Local SQLite liquidity snapshots while live terminal is open. Set BTC_PRO_DB to a persistent mounted path if available. Official BRTI not integrated. ML forecasts are separate from target heuristic; no calibrated settlement ML, live network testing, or automated settlement verification.

## Optional official BRTI (read-only)
In Streamlit app Settings > Secrets, add:
```toml
[kalshi]
key_id = "YOUR_KEY_ID"
private_key = """
-----BEGIN PRIVATE KEY-----
YOUR_PRIVATE_KEY_CONTENT
-----END PRIVATE KEY-----
"""
```
Use the exact RSA PEM downloaded from Kalshi, including its original header/footer. Never commit secrets to GitHub. Restrict app access and key permissions where possible. Restart after changing secrets. The app only subscribes to index data; it cannot submit orders. The authenticated connection requires account access. Missing/stale ticks are explicitly labeled and the forecast reverts to the exchange proxy. The final-minute average is displayed but is not incorporated into the forecast or automatic settlement; existing ML estimates remain separate. Consult https://docs.kalshi.com/websockets/cfbenchmarks-value for feed details and terms before redistributing data.
