from flask import Flask, Markup, render_template, jsonify
#from flask import Flask,jsonify,request
#from flask import render_template
#import ast
app = Flask(__name__)
#labels = []
#values = []

labels = [
    '2020-03-03 13:05:00'
    , '2020-03-03 13:10:00'
    , '2020-03-03 13:15:00'
    , '2020-03-03 13:20:00'
    , '2020-03-03 13:25:00'
    , '2020-03-03 13:30:00'
    , '2020-03-03 13:35:00'
    , '2020-03-03 13:40:00'
    , '2020-03-03 13:45:00'
    , '2020-03-03 13:50:00'
    , '2020-03-03 13:55:00'
    , '2020-03-03 14:00:00'
]

values = [
    967.67, 1190.89, 1079.75, 1349.19,
    2328.91, 2504.28, 2873.83, 4764.87,
    4349.29, 6458.30, 9907, 16297
]

@app.route('/')
def line():
    global labels,values
    #labels = []
    #values = []
    return render_template('line_chart.html', title='Google stock price over time', max=17000, labels=labels, values=values)

@app.route('/refreshData')
def refresh_graph_data():
    global labels, values
    print("labels now: " + str(labels))
    print("data now: " + str(values))
    from random import randint
    values = [randint(0,17000)]*len(values)
    print(values)
    return jsonify(sLabel=labels, sData=values)

# Todo - figure out why auto-updating the chart doesn't work
# maybe the order of the script? check with Hanee's work
# maybe the chart library?

#@app.route('/updateData', methods=['POST'])
#def update_data():
#	global labels, values
#	if not request.form or 'data' not in request.form:
#    	return "error",400
#	labels = ast.literal_eval(request.form['label'])
#	values = ast.literal_eval(request.form['data'])
#	print("labels received: " + str(labels))
#	print("data received: " + str(values))
#	return "success",201

if __name__ == "__main__":
	app.run(host='0.0.0.0', debug=True, port=9009)
