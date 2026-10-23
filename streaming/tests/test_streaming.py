"""Small unit tests for sink transformation helpers; run with pytest."""
from common.redis_keys import user_recent, user_bought, encode_recent

def test_redis_key_contract():
    assert user_recent("42") == "user:recent:42"
    assert user_bought("42") == "user:bought:42"

def test_recent_member_contract():
    assert encode_recent("123", "view", 1720000000) == "123|view|1720000000"
