import time
import ipaddress
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed


def test_host_service(ip, port, timeout=0.5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        socket.setdefaulttimeout(timeout)
        # returns an error indicator
        result = s.connect_ex((ip, port))
        if result == 0:
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False
    finally:
        s.close()


DATABASE_PORT_NAME_DICT = {
    3306: 'MySql/MariaDB',
    1521: 'Oracle',
    1433: 'SqlServer',
    50000: 'DB2',
    9088: 'Informix/Gbase',
    12345: 'DM',
    54321: 'Kingbase',
    9090: 'Hbase',
    6379: 'Redis',
    27017: 'MongoDB',
    5866: 'HighGo',
    1025: 'Teradata',
    5432: 'PostgreSql/Greenplum/ux',
    5000: 'Sybase',
    9042: 'Cassandra',
    2003: 'Shentong/Shenzhou',
    5138: 'Xugu',
    7474: 'Neo4jv3',
    2480: 'orient',
    10000: 'hive',
    9200: 'ElasticSearch',
    1972: 'cache',
    39015: 'Hana',
    8000: 'Gaussdb',
    30004: 'Tbase',
    4000: 'Tidb',
    8182: 'TinkerPop'
}


def ip2num(ip):
    ips = [int(x) for x in ip.split('.')]
    return ips[0] << 24 | ips[1] << 16 | ips[2] << 8 | ips[3]


def num2ip(num):
    return '%s.%s.%s.%s' % ((num >> 24) & 0xff, (num >> 16) & 0xff, (num >> 8) & 0xff, (num & 0xff))


def generate_ip(ip):
    start, end = [ip2num(x) for x in ip.split('-')]
    return [num2ip(num) for num in range(start, end+1) if num & 0xff]


def get_iplists(ip_range):
    try:
        ip_range = ip_range.replace('_', '/')
        if "/" in ip_range:
            net = ipaddress.ip_network(ip_range, strict=False)
            ips = [str(item) for item in net.hosts()]
        else:
            ips = generate_ip(ip_range)
    except ValueError:
        return []
    except Exception:
        return []
    return ips


def has_service(info):
    ip, port, database_type = info
    service = False
    try:
        service = test_host_service(ip, port)
    except Exception as e:
        print(e)

    result = []
    if service:
        name = DATABASE_PORT_NAME_DICT.get(
            port, '') if database_type == 'Unlimited' else database_type

        result.append({
            'ip': ip,
            'port': port,
            'name': name
        })

    return result


def single_database_found(ip, database_type, ports):
    if database_type == 'Unlimited':
        ports = DATABASE_PORT_NAME_DICT.keys()
    else:
        if not ports:
            return []
    all_task = []
    pool = ThreadPoolExecutor(max_workers=20)
    for port in ports:
        task = pool.submit(has_service, (str(ip), port, database_type))
        all_task.append(task)
    result = []
    for future in as_completed(all_task):
        data = future.result()
        result.extend(data)
    return result


def database_found(ip_range, database_type, ports):
    ips = get_iplists(ip_range)
    all_task = []
    pool = ThreadPoolExecutor(max_workers=256)
    for ip in ips:
        task = pool.submit(single_database_found, ip, database_type, ports)
        all_task.append(task)
    result = []
    for future in as_completed(all_task):
        data = future.result()
        if data:
            result.extend(data)
    return result


if __name__ == '__main__':
    now = time.time
    start = now()
    # print(database_found('192.168.37.0_255.255.255.0', database_type='Unlimited', ports=[]))
    # print(database_found('192.168.37.0/24', database_type='Unlimited', ports=[]))
    results = database_found('192.168.37.0_255.255.255.0',
                             database_type='custom', ports=[3306])
    print(len(results))
    print("Time:", now()-start)
