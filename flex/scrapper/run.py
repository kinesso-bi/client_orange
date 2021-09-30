import json


def get_credentials():
    try:
        with open('credentials.json') as file:
            credentials = json.load(file)
        return credentials
    except FileNotFoundError as f:
        return None


user = get_credentials()['username']
password = get_credentials()['password']

print(user, password)


