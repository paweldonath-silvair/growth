from datetime import datetime

from influxdb import InfluxDBClient
from influxdb.resultset import ResultSet


def influx_client(dirname="secret") -> InfluxDBClient:
    url = "mcfly-bc026f47.influxcloud.net"
    with open(f"{dirname}/influx_user.txt") as f:
        user = f.read()
    with open(f"{dirname}/influx_password.txt") as f:
        password = f.read()

    client = InfluxDBClient(host=url, port=8086, username=user, password=password, ssl=True)
    return client


def timestamp(d: datetime):
    return int(d.timestamp()) * 10**9


def get_data(database: str, measurement: str, time_range=None, order='DESC', limit=10):
    client = influx_client()
    client.switch_database(database)
    query = f'SELECT * FROM {measurement}'
    if time_range is not None:
        query += f' WHERE time >= {timestamp(time_range[0])} and time < {timestamp(time_range[1])}'
    if order.upper() in ['DESC', 'ASC']:
        query += f' ORDER BY time {order}'
    if limit is not None:
        query += f' LIMIT {limit}'
    results: ResultSet = client.query(query)
    return list(results.get_points())


def test():
    print(get_data('dev_agg', 'energy_agg'))


if __name__ == "__main__":
    test()
