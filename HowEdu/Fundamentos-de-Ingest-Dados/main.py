# %%
import random
import backoff
import requests
import json
import logging

url = 'https://economia.awesomeapi.com.br/json/last/USD-BRL'
ret = requests.get(url)

if ret:
    print(ret)
else:
    print("Falhou")


json.loads(ret.text)

dolar = json.loads(ret.text)['USDBRL']

print(f" 20 Dólares hoje custam {float(dolar['bid']) * 20} reais")

# %%


def cotacao(valor, moeda):
    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    # url = 'https://economia.awesomeapi.com.br/json/last/{}'.format(moeda)
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")


# %%
cotacao(20, 'USD-BRL')
# %%
cotacao(20, 'JPY-BRL')
# %%
cotacao(20, 'Rhuan')
# %%
try:
    cotacao(20, 'Rhuan')
except:
    pass
# %%
try:
    cotacao(20, 'Rhuan')
except:
    pass
else:
    print('ok')

# %%
try:
    cotacao(20, 'JPY-BRL')
except:
    pass
else:
    print('ok')

# %%
try:
    cotacao(20, 'RHUAN')
except Exception as e:
    print(e)
else:
    print('ok')

# %%
try:
    10/0
except Exception as e:
    print(e)
else:
    print('ok')

# %%
lst_money = ["USD-BRL",
             "EUR-BRL",
             "BTC-BRL",
             "JPY-BRL",
             "RPL-BRL"]
valor = 20

for moeda in lst_money:
    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")
# %%
lst_money = ["USD-BRL",
             "EUR-BRL",
             "BTC-BRL",
             "RPL-BRL",
             "JPY-BRL"
             ]
valor = 20

for moeda in lst_money:
    try:
        url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
        ret = requests.get(url)
        dolar = json.loads(ret.text)[moeda.replace('-', '')]
        print(
            f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")
    except:
        print(f"falha na moeda: {moeda}")
# %%


def multi_moeda(valor, moeda):

    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")


multi_moeda(20, "USD-BRL")
# %%


def error_check(func):
    def inner_func(*args, **kargs):
        try:
            func(*args, **kargs)
        except:
            print(f"{func.__name__} falhou")
    return inner_func


multi_moeda(20, "USD-BRL")
multi_moeda(20, "USD-BRL")
multi_moeda(20, "USD-BRL")
multi_moeda(20, "USD-BRL")


# %%

url = 'https://economia.awesomeapi.com.br/json/last/USD-BRL'
ret = requests.get(url)

if ret:
    print(ret)
else:
    print("Falhou")


json.loads(ret.text)

dolar = json.loads(ret.text)['USDBRL']

print(f" 20 Dólares hoje custam {float(dolar['bid']) * 20} reais")

# %%


def cotacao(valor, moeda):
    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    # url = 'https://economia.awesomeapi.com.br/json/last/{}'.format(moeda)
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")


# %%
cotacao(20, 'USD-BRL')
# %%
cotacao(20, 'JPY-BRL')
# %%
cotacao(20, 'Rhuan')
# %%
try:
    cotacao(20, 'Rhuan')
except:
    pass
# %%
try:
    cotacao(20, 'Rhuan')
except:
    pass
else:
    print('ok')

# %%
try:
    cotacao(20, 'JPY-BRL')
except:
    pass
else:
    print('ok')

# %%
try:
    cotacao(20, 'RHUAN')
except Exception as e:
    print(e)
else:
    print('ok')

# %%
try:
    10/0
except Exception as e:
    print(e)
else:
    print('ok')

# %%
lst_money = ["USD-BRL",
             "EUR-BRL",
             "BTC-BRL",
             "JPY-BRL",
             "RPL-BRL"]
valor = 20

for moeda in lst_money:
    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")
# %%
lst_money = ["USD-BRL",
             "EUR-BRL",
             "BTC-BRL",
             "RPL-BRL",
             "JPY-BRL"
             ]
valor = 20

for moeda in lst_money:
    try:
        url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
        ret = requests.get(url)
        dolar = json.loads(ret.text)[moeda.replace('-', '')]
        print(
            f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")
    except:
        print(f"falha na moeda: {moeda}")
# %%


def multi_moeda(valor, moeda):

    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")


multi_moeda(20, "USD-BRL")
# %%


def error_check(func):
    def inner_func(*args, **kargs):
        try:
            func(*args, **kargs)
        except:
            print(f"{func.__name__} falhou")
    return inner_func


@error_check
def multi_moeda(valor, moeda):

    url = f'https://economia.awesomeapi.com.br/json/last/{moeda}'
    ret = requests.get(url)
    dolar = json.loads(ret.text)[moeda.replace('-', '')]
    print(
        f"{valor} {moeda[:3]} hoje custam {float(dolar['bid']) * valor} {moeda[-3:]}")


# %%
multi_moeda(20, "USD-BRL")
multi_moeda(20, "EUR-BRL")
multi_moeda(20, "BTC-BRL")
multi_moeda(20, "RPL-BRL")
multi_moeda(20, "JPY-BRL")

# %%
@backoff.on_exception(backoff.expo, (ConnectionAbortedError, ConnectionRefusedError, TimeoutError), max_tries=10)
def test_func(*args, **kwargs):
    rnd = random.random()
    print(f"""
            RND: {rnd}
            args: {args if args else 'sem args'}
            kwargs: {kwargs if kwargs else 'sem kwargs'}
          """)
    if rnd < .2:
        raise ConnectionAbortedError('Conexão foi finalizada')
    elif rnd < .4:
        raise ConnectionRefusedError('Conexão foi recusada')
    elif rnd < .6:
        raise TimeoutError('Tempo de espera excedido')
    else:
        return "OK!"


# %%
test_func()
# %%
test_func(42)

# %%
test_func(42, 51, nome='rhuan')

# %%
log = logging.getLogger()
log.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
log.addHandler(ch)
# %%
@backoff.on_exception(backoff.expo, (ConnectionAbortedError, ConnectionRefusedError, TimeoutError), max_tries=10)
def test_func(*args, **kwargs):
    rnd = random.random()
    log.debug(f" RND: {rnd} ")
    log.info(f"args: {args if args else 'sem args'}")
    log.info(f"kwargs: {kwargs if kwargs else 'sem kwargs'}")
          
    if rnd < .2:
        log.error('Conexão foi finalizada')
        raise ConnectionAbortedError('Conexão foi finalizada')
    elif rnd < .4:
        log.error('Conexão foi recusada')
        raise ConnectionRefusedError('Conexão foi recusada')
    elif rnd < .6:
        log.error('Tempo de espera excedido')
        raise TimeoutError('Tempo de espera excedido')
    else:
        return "OK!"
# %%
test_func()

# %%
