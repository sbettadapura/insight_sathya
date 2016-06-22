from app import app
from flask import jsonify
import redis
import sys
from operator import add
@app.route('/')

@app.route('/index')

def index():

  return "Hello, World!"

@app.route('/api/current/query')

def get_redis_stats():
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in ("calls*", "num*", "race*")]
	return jsonify(reduce(add, d))
