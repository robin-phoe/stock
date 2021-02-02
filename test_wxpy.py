from wxpy import *
import time

bot = Bot(cache_path=True)
# bot = Bot()

# 机器人账号自身
for i in range(10):
    image_path = 'test.jpg'
    # my_friend = bot.groups().search(u'7个涨停翻一番')[0]
    my_friend = bot.friends().search(u'7个涨停翻一番')[0]
    my_friend.send_image(image_path)
    time.sleep(10)
if __name__ == '__main__':
    pass