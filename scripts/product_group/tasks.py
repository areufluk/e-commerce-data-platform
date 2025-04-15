import os


def test_call_function():
    SERVICE_ACCOUNT_KEY_ID = os.environ.get("service-account.json")
    print(SERVICE_ACCOUNT_KEY_ID)
