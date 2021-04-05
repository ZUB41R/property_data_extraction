from cryptography.fernet import Fernet
import pickle

def decrypt_secrets():
    directory = "keepitsecret/"
    key_file = f"{directory}key.bin"
    secret_file = f"{directory}secrets.bin"
    with open(key_file, 'rb') as kf:
        key = kf.read()
    s_key = Fernet(key)

    with open(secret_file, 'rb') as sf:
        secret_dict = pickle.load(sf)

    for key, value in secret_dict.items():
        secret_dict[key] = s_key.decrypt(value).decode("utf-8")

    return secret_dict

