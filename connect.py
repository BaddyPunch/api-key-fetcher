import requests
import json
from datetime import datetime

# Переменная для хранения API ключа и времени получения
api_key_cache = {
    "timestamp": None,
    "api_key": None
}


def get_apikey(CB_user='login', CB_pwd='password'):

    current_timestamp = int(datetime.now().timestamp())
    last_timestamp = api_key_cache['timestamp']

    # Проверка, истек ли API ключ или его нет
    if last_timestamp is None or (current_timestamp - last_timestamp) >= 43200:
        url = "https://api3.casebook.ru/login"
        payload = json.dumps({
            "login": CB_user,
            "password": CB_pwd
        })
        headers = {
            'Content-Type': 'application/json'
        }

        # Выполняем POST запрос для получения нового ключа
        response = requests.post(url, headers=headers, data=payload)

        if response.status_code == 200:
            resp = response.json()
            api_key = resp['apiKeys'][0]  # Получаем ключ из ответа

            # Обновляем кэш с новым ключом и временной меткой
            api_key_cache['timestamp'] = current_timestamp
            api_key_cache['api_key'] = api_key

            return api_key
        else:
            print(f"Ошибка получения API ключа: {response.status_code}, {response.text}")
            return None
    else:
        # Если ключ ещё актуален, возвращаем его
        return api_key_cache['api_key']

