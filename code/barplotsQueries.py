import numpy as np
import re
import matplotlib.pyplot as plt 
import matplotlib


labels = ['Query1', 'Query2', 'Query3', 'Query4', 'Query5']

times = []

with open('times.txt','r') as f:
    for line in f:
        times.append(float(line.strip()))

rdd = times[0:5]
sql_csv = times[5:10]
sql_pqt = times[10:15]
x = np.arange(len(labels))  # the label locations

width = 0.3  # the width of the bars

fig, ax = plt.subplots()
fig.set_size_inches(14, 10)

rects1 = ax.bar(x - width, rdd, width, label='RDD', color='tab:blue' )
rects2 = ax.bar(x , sql_csv, width, label='sqlCSV', color='purple')
rects3 = ax.bar(x + width, sql_pqt, width, label='sqlPQT', color='crimson')


# Add some text for labels, title and custom x-axis tick labels, etc.
ax.set_ylabel('Time (s)')
ax.set_title('Runtime')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()


def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')


autolabel(rects1)
autolabel(rects2)
autolabel(rects3)

fig.tight_layout()

plt.savefig("barplots_q.jpg")
plt.show()





