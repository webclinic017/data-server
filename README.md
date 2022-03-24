<div align="center">
    <h1>OpenCTA Data Server</h1>
</div>

# Features

- REST API for data
- TLS encryption
- Data cached in S3 (Minio) storage
- Clear data secured with API key
- Obfuscated data in open API

# Quick Start

## Pre-requisites

Set up a Linux server. You can rent one at OVH.

On the Linux server, install Docker:

```bash
sudo apt install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository  "deb [arch=amd64] https://download.docker.com/linux/ubuntu  $(lsb_release -cs) stable"
sudo apt install -y docker.io
```

Verify that Docker CE is installed correctly by running the hello-world image.

```bash
sudo docker run hello-world
```

Set up Docker to be able to execute it without sudo:

```bash
sudo usermod -aG docker ${USER}
su - ${USER}
```

Install docker-compose.

```bash
sudo curl -L https://github.com/docker/compose/releases/download/1.27.4/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```


## Installation

On the Linux server:

- Clone the repo:

```bash
git clone https://github.com/OpenCTA-com/data-server.git
cd data-server
```

- Duplicate `docker-compose.yml.template` to `docker-compose.yml`.
- Edit the configuration file `docker-compose.yml` and set your own environment variables values replacing the `{{ XXX }}` placeholders.


## TLS encryption (optional)

If you want to use TLS encryption (which is recommended), you need to generate SSL certificates:

- Update your DNS registry (in our case, on https://domains.google.com/registrar/opencta.com/dns) to make a sub domain (`data.opencta.com` in our case) point to the Linux server.

On a Linux/MacOS machine:

- Install certbot:
  - MacOS: `brew install certbot`
  - Ubuntu: `sudo apt install certbot` 
- Run:

```bash
certbot -d data.opencta.com --manual --logs-dir certbot --config-dir certbot --work-dir certbot --preferred-challenges dns certonly
```

- Copy the following files to the Windows server in the `certificates` folder:
  - `certbot/live/data.opencta.com/fullchain.pem`
  - `certbot/live/data.opencta.com/privkey.pem`


## Server launch

On the Linux server:



## Usage

Now you should be able to query data with:

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

You can access the API documentation from the Internet: https://data.opencta.com/docs.


