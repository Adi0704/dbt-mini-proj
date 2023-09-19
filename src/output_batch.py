import json
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer
import time
import threading

plt_val_x = []
plt_val_temp = []
plt_val_apptemp = []
plt_val_pres = []
plt_val_rh = []
city_name = 'Chennai'
buffer = []
buffer_size = 3

def animate(i):
    global plt_val_x, plt_val_temp, plt_val_apptemp, plt_val_pres, plt_val_rh, buffer

    if len(buffer) == buffer_size:
        for b in buffer:
            plt_val_x.append(b[0])
            plt_val_temp.append(b[1])
            plt_val_apptemp.append(b[2])
            plt_val_pres.append(b[3])
            plt_val_rh.append(b[4])

        buffer.clear()

        for ax in axs.flat:
            ax.cla()

        axs[0, 0].plot(plt_val_x, plt_val_temp, color='blue')
        axs[0, 0].set_title('Temperature ({} : {})'.format(min(plt_val_temp), max(plt_val_temp)))
        axs[0, 1].plot(plt_val_x, plt_val_apptemp, color='green')
        axs[0, 1].set_title('Feels like ({} : {})'.format(min(plt_val_apptemp), max(plt_val_apptemp)))
        axs[1, 0].plot(plt_val_x, plt_val_pres, color='red')
        axs[1, 0].set_title('Pressure ({} : {})'.format(min(plt_val_pres), max(plt_val_pres)))
        axs[1, 1].plot(plt_val_x, plt_val_rh, color='purple')
        axs[1, 1].set_title('Relative Humidity ({} : {})'.format(min(plt_val_rh), max(plt_val_rh)))
        plt.style.use('fivethirtyeight')

def plot():
    global consumer, buffer, buffer_size
    count = 0
    for message in consumer:
        if message.value['city'] == city_name:
            count += 5
            buffer.append((count, float(message.value["temp"]), float(message.value["app_temp"]), float(message.value["pres"]), float(message.value["rh"])))

            if len(buffer) >= buffer_size:
                time.sleep(1)

consumer = KafkaConsumer('output', bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf-8')))

plot_thread = threading.Thread(target=plot).start()

time.sleep(10)
fig, axs = plt.subplots(2, 2, figsize=(12, 6), num=city_name)
ani = FuncAnimation(plt.gcf(), animate, interval=1000)
plt.tight_layout()
plt.show()
