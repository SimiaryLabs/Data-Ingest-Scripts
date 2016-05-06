import numpy as np

dump = "ab.npy"

articles = np.load(dump)

count = 0
for article in articles:
	print(count, article["title"])
	print(count, article["sections"])
	count = count + 1
	if (count == 1):
		break