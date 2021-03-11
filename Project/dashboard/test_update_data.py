import requests

new_data = {'tickers' : str(['fb', 'nvda']), 'ticker_ts_str' : str([['2021-03-04 15:00:00', '2021-03-04 15:25:00', '2021-03-04 15:30:00'], ['2021-03-04 15:20:00', '2021-03-04 15:25:00', '2021-03-04 15:30:00']]), 'n_tweets' : str([[3,4,5], [4,4,2]]), 'price': str([[100,200,300],[150,220,310]])}


url = 'http://0.0.0.0:9009/updateData'

response = requests.post(url, data=new_data)
