<div align="center">
    <h1>OpenCTA Data Server</h1>
</div>

# Features

# Usage

```bash
curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/carry/bond?ticker=CGB&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/carry/commodity?ticker=CL&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/carry/currency?ticker=AD&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/carry/equity?ticker=ES&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/cot?ticker=LH&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/currency?ticker=CGB&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/factor/roll-return?ticker=AD&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/nav/long?ticker=AD&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/nav/short?ticker=AD&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/ohlcv?ticker=C&start_date=2022-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/daily/splits?ticker=AD&start_date=2012-01-01&end_date=2022-02-28

curl -H "Authorization: $DATA_SECRET_KEY" \
  https://data.opencta.com/tickers

```