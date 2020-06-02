import json
import dateutil.parser
import re
from datetime import timedelta
import datetime
from influxdb import InfluxDBClient

# Чтение json
with open("input.json", "r") as read_file:
    json_data = json.load(read_file)

# Подключение к БД
client = InfluxDBClient(host=json_data['host'], port=json_data['port'])
client.switch_database(json_data['database'])

# Запрос на получение VU
stages_query = client.query("SELECT maxAT FROM jmeter "
                            "WHERE time >= $startTime AND time <= $endTime",
                            bind_params={"startTime": json_data['startTime'], "endTime": json_data['endTime']})

stages_points = list(stages_query.get_points())

def times(testStart, testEnd, VU):
    min_query = client.query("SELECT FIRST(maxAT) FROM jmeter "
                            "WHERE time >= $startTime AND time <= $endTime AND \"maxAT\" = $VU ",
                            bind_params={"startTime": testStart, "endTime": testEnd, "VU": VU})

    max_query = client.query("SELECT LAST(maxAT) FROM jmeter "
                            "WHERE time >= $startTime AND time <= $endTime AND \"maxAT\" = $VU ",
                            bind_params={"startTime": testStart, "endTime": testEnd, "VU": VU})

    min_points = list(min_query.get_points())
    max_points = list(max_query.get_points())

    return dateutil.parser.parse(min_points[0]['time']).strftime("%Y-%m-%d %I:%M:%S"), \
           dateutil.parser.parse(max_points[0]['time']).strftime("%Y-%m-%d %I:%M:%S")


# Будущий список с временами time_list[i][j],
# где i - номер ступени, начиная с 0. При j = 0 - время начала ступени, при j = 1 - время конца ступени
time_list = []

for i in range(len(json_data['stages'])):
    time_list.append(times(json_data['startTime'], json_data['endTime'], json_data['stages'][i]['VU']))


# 2-7 минута ступени
def two_minutes_of_stage(stage_start_time):
    a = stage_start_time
    a = re.split('[-: ]', a)
    a = list(map(int, a))
    aa = datetime.datetime(a[0], a[1], a[2], a[3], a[4], a[5])

    sTime = aa + timedelta(minutes=2)
    return str(sTime)


def seven_minutes_of_stage(stage_start_time):
    a = stage_start_time
    a = re.split('[-: ]', a)
    a = list(map(int, a))
    aa = datetime.datetime(a[0], a[1], a[2], a[3], a[4], a[5])

    eTime = aa + timedelta(minutes=7)
    return str(eTime)


# Функция интенсивность транзации
def request(transaction_name, startTime, endTime):
    count_query = client.query("SELECT count, countError FROM jmeter WHERE time >= $startTime AND time <= $endTime "
                               "AND \"transaction\" = $transaction",
                               bind_params={"startTime": startTime, "endTime": endTime,
                                            "transaction": transaction_name})

    count_points = list(count_query.get_points())

    amountTransaction = 0
    error = 0
    for i in range(len(count_points)):

        amountTransaction += count_points[i]['count']
        if (count_points[i]['countError'] != None):
            error += count_points[i]['countError']

    print('Transaction: {}, Amount/h: {}, Success/h: {}, Error/h: {}'
          .format(transaction_name, amountTransaction * 12, (amountTransaction - error) * 12, error * 12))

    return amountTransaction * 12, amountTransaction


# Список SLA транзакции, возвращает SLA в секундах
def sla_request(transaction_name, startTime, endTime):
    sla_query = client.query("SELECT \"pct90.0\" FROM jmeter WHERE \"transaction\" = $transaction "
                             "AND \"statut\" = 'ok' AND time >= $startTime AND time <= $endTime",
                             bind_params={"startTime": startTime, "endTime": endTime, "transaction": transaction_name})
    sla_points = list(sla_query.get_points())

    slaPoints = []

    for i in range(len(sla_points)):
        slaPoints.append(round(sla_points[i]['pct90.0'] / 1000, 2))

    return slaPoints


# Проверка SLA
'''
for x in range(len(json_data['stages'])):
    print('Stage {}'.format(x))
    for i in range(len(json_data['transactions'])):
        print('Транзакция: {}'.format(json_data['transactions'][i]['name']))

        for j in range(len(sla_request(json_data['transactions'][i]['name'], time_list[i][0], time_list[i][1]))):
            if sla_request(json_data['transactions'][i]['name'], time_list[i][0], time_list[i][1])[j] > json_data['transactions'][i]['SLA']:
                print(json_data['transactions'][i]['SLA'])
                print('Превышен sla')
                break
'''

# Проверка интенсивности


for x in range(len(json_data['stages'])):
    print('Stage {}'.format(x))
    for i in range(len(json_data['transactions'])):
        if request(json_data['transactions'][i]['name'], time_list[x][0], time_list[x][1])[0] < json_data['transactions'][i]['intence']:
            print('Минимальная треб. интенсивность {}'.format(json_data['transactions'][i]['intence']))


#print(request(json_data['transactions'][0]['name'], time_list[0][0], time_list[0][1])[0])
#print(json_data['transactions'][0]['intence'])
#print(request(json_data['transactions'][0]['name'], time_list[1][0], time_list[1][1])[0])

def min_max_intencity():
    for i in range(1, len(json_data['stages']) + 1):
        print(json_data['transactions'][i-1]['name'])
        print('min')
        print((json_data['transactions'][i-1]['intence'] * (i / 10) + json_data['transactions'][0]['intence'])
              - (json_data['transactions'][i-1]['intence'] * (i / 10) + json_data['transactions'][0]['intence']) * 0.05)
        print('max')
        print(json_data['transactions'][i-1]['intence'] * (i / 10) + json_data['transactions'][0]['intence'])


min_max_intencity()


'''
for i in range(1, len(json_data['stages']) + 1):
    print('min')
    print((11000 * (i / 10) + 11000) - ((11000 * (i / 10) + 11000) * 0.05))
    print('max')
    print(11000 * (i / 10) + 11000)
'''

'''
#110% мин-макс
print((json_data['transactions'][0]['intence'] * 0.1 + json_data['transactions'][0]['intence'] )
- (json_data['transactions'][0]['intence'] * 0.1 + json_data['transactions'][0]['intence']) * 0.05)
print(json_data['transactions'][0]['intence'] * 0.1 + json_data['transactions'][0]['intence'])

#120% мин-макс
print((json_data['transactions'][0]['intence'] * 0.2 + json_data['transactions'][0]['intence'] )
      - (json_data['transactions'][0]['intence'] * 0.2 + json_data['transactions'][0]['intence']) * 0.05)
print(json_data['transactions'][0]['intence'] * 0.2 + json_data['transactions'][0]['intence'])
'''

#if request(json_data['transactions'][0]['name'], time_list[0][0], time_list[0][1])[0] < 11000:
#    print('< 11000')
#print(request(json_data['transactions'][0]['name'], time_list[0][0], time_list[0][1])[0])
#print(request(json_data['transactions'][0]['name'], time_list[0][0], time_list[0][1])[1])

'''
print('sla2')
for i in range(len(json_data['transactions'])):
    print('Транзакция: {}'.format(json_data['transactions'][i]['name']))

    for j in range(len(sla_request(json_data['transactions'][i]['name'], t21, t22))):
        if sla_request(json_data['transactions'][i]['name'], t21, t22)[j] > json_data['transactions'][i]['SLA']:
            print('Превышен sla')
            break

print('sla3')
for i in range(len(json_data['transactions'])):
    print('Транзакция: {}'.format(json_data['transactions'][i]['name']))

    for j in range(len(sla_request(json_data['transactions'][i]['name'], t31, t32))):
        if sla_request(json_data['transactions'][i]['name'], t31, t32)[j] > json_data['transactions'][i]['SLA']:
            print('Превышен sla')
            break

print('sla4')
for i in range(len(json_data['transactions'])):
    print('Транзакция: {}'.format(json_data['transactions'][i]['name']))

    for j in range(len(sla_request(json_data['transactions'][i]['name'], t41, t42))):
        if sla_request(json_data['transactions'][i]['name'], t41, t42)[j] > json_data['transactions'][i]['SLA']:
            print('Превышен sla')
            break

print('sla5')
for i in range(len(json_data['transactions'])):
    print('Транзакция: {}'.format(json_data['transactions'][i]['name']))

    for j in range(len(sla_request(json_data['transactions'][i]['name'], t51, t52))):
        if sla_request(json_data['transactions'][i]['name'], t51, t52)[j] > json_data['transactions'][i]['SLA']:
            print('Превышен sla')
            break

# сделать нормальное оформление
print("Operations per hour")
for i in range(len(json_data['transactions'])):
    if (request(json_data['transactions'][i]['name'], two_minutes_of_stage(t11), seven_minutes_of_stage(t11))[0] >=
            json_data['transactions'][i]['intence']):
        print('>=', json_data['transactions'][i]['intence'])
    else:
        print('<=', json_data['transactions'][i]['intence'])
'''
'''
for i in range(len(json_data['transactions'])):
    if (json_data['stages'][1]['VU'] == 20):
        request(json_data['transactions'][i]['name'], t21, fiveMinutesOfStage(t21))
print()

for i in range(len(json_data['transactions'])):
    if (json_data['stages'][2]['VU'] == 30):
        request(json_data['transactions'][i]['name'], t31, fiveMinutesOfStage(t31))
print()

for i in range(len(json_data['transactions'])):
    if (json_data['stages'][3]['VU'] == 40):
        request(json_data['transactions'][i]['name'], t41, fiveMinutesOfStage(t41))
print()

for i in range(len(json_data['transactions'])):
    if (json_data['stages'][4]['VU'] == 50):
        request(json_data['transactions'][i]['name'], t51, fiveMinutesOfStage(t51))
print()
'''
