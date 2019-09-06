import redis
from config import REDIS_HOST, REDIS_PORT

pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT)

redis_client = redis.Redis(connection_pool=pool)

# 释放锁lua脚本
release_lock_script = """
if redis.call('get', KEYS[1]) == ARGV[1] 
    then 
        return redis.call('del', KEYS[1]) 
    else 
        return 0
end
"""


def release_redis_lock(lock_key_name, lock_value):
    script_client = redis_client.register_script(release_lock_script)
    return script_client(keys=[lock_key_name], args=[lock_value])
