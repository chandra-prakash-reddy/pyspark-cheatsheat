apt-get update

apt-get install -y unzip
apt-get install -y wget
apt-get install -y gnupg2
apt-get install -y curl
apt-get install -y g++
apt-get install -y unixodbc-dev


#install mssql clinet
apt-get install -y mariadb-client
apt install default-libmysqlclient-dev
echo "MYSQL CLIENT INSTALLED.....!"

#install oracle client
wget https://download.oracle.com/otn_software/linux/instantclient/19600/instantclient-basic-linux.x64-19.6.0.0.0dbru.zip
unzip instantclient-basic-linux.x64-19.6.0.0.0dbru.zip -d oracle_client
export ORACLE_HOME=/oracle_client/instantclient_19_6
export LD_LIBRARY_PATH=/oracle_client/instantclient_19_6
echo /oracle_client/instantclient_19_6 > /etc/ld.so.conf.d/oracle-instantclient.conf
ldconfig
echo "ORACLE CLIENT INSTALLED.....!"

# install mssql client
curl https://packages.microsoft.com/keys/microsoft.asc |  apt-key add -
echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main" | tee /etc/apt/sources.list.d/mssql-release.list
apt update
apt install -y msodbcsql17
echo "MSSQL CLIENT INSTALLED.....!"


docker container commit <> python3.7-slim
