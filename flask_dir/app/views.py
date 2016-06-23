from app import app
from flask import jsonify
from flask import Flask, render_template
import redis
import sys
from operator import add

@app.route('/')

@app.route('/index')

def index():

  return "Hello, World!"


def get_redis_stats():
	r = redis.Redis(host = sys.argv[1], port = 6379, password = 'noredishackers')
	d = [[(k, r.get(k)) for k in r.keys(keys)] for keys in ("calls*", "num*", "race*")]
	return reduce(add, d)

@app.route('/api/current/query')
def current_query():
	d = get_redis_stats()
	return jsonify(d)


@app.route('/api/barchart')
def barchart(chartID = 'chart_ID', chart_type = 'bar', chart_height = 350):
	d = get_redis_stats()
	dx = [{"name": x[0],"data": [int(x[1])]} for x in d]
	chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
	#series = [{"name": 'Label1', "data": [1,2,3]}, {"name": 'Label2', "data": [4, 5, 6]}]
	series = dx
	title = {"text": 'Commute Stats'}
	yAxis = {"categories": ['Calls', 'Size', 'Race Conditions']}
	xAxis = {"title": {"text": 'Current Traffic Stats'}}
	return render_template('index.html', chartID=chartID, chart=chart, series=series, title=title, xAxis=xAxis, yAxis=yAxis)
