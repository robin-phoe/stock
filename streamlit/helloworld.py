#coding=utf-8
import streamlit as st
import pandas as pd
import numpy as np
import datetime
import time
#from cpu_memory import read_disk
st.sidebar.title(u'菜单侧边栏')

def machine():
    st.title('Steamlit web app')
    st.write('hello world')
    x=st.slider('x')
    st.write(x,'squared is',x*x)
    st.markdown(u'- 按钮')
    if st.button('Say hello'):
        st.write('Why hello there')
@st.cache(persist=True)
def data():
    dicts={'cpu':[],'memery':[]}
    df_stadio=df_value=pd.DataFrame(dicts,index=[])
    for i in range(10):
        df_stadio.loc[datetime.datetime.now()]=[i,2]
        time.sleep(1)
    # df = pd.DataFrame(
    # np.random.randn(200, 3),
    # columns=['a', 'b', 'c'])
    # df_stadio=pd.read_csv('c_m_ration.csv',index_col=0)
    # df_value=pd.read_csv('m_value.csv',index_col=0)
    return df_stadio,df_value
add_button1=st.sidebar.button(u'总览')
# add_selectbox
machine_chose = st.sidebar.selectbox(
    u"按机器：",
    ("85", "31", "32")
)
st.write(machine_chose)
st.button('re-run')
mac_in_button=st.sidebar.button(u'查看')
# add_selectbox
work_chose = st.sidebar.selectbox(
    u"按业务：",
    (u"报表", u"行情")
)
work_in_button=st.sidebar.button(u'查看1')
# add_selectbox
work_control = st.sidebar.selectbox(
    u"工作台：",
    (u"85", u"225")
)
if work_control=='85':
    st.markdown('- 文本输入')
    title = st.text_input('Movie title', '')
    txt = st.text_area('Text to analyze', title)
    st.write(title)
control_link=st.sidebar.button(u'连接')
if mac_in_button:
    # st.subheader('cpu&memory:')
    # st.write(machine_chose)
    df_stadio,df_value=data()
    # st.line_chart(df)
    # st.line_chart(df)
    st.write(datetime.datetime.now())
    st.line_chart(df_stadio, width=10, height=400)
    #disk_dict=read_disk()
    #st.bar_chart(disk_dict, width=10000, height=500)
    st.bar_chart(df_value, width=10000, height=400)
a=0
if control_link:
    a=555
    # input_txt = st.text_input(u'命令：', value='a', key=None)
    #print(input_txt)
    # do_button = st.button('Do')
    # if do_button:
    #     pass
    # return_txt=st.text_area(u'命令返回', value=input_txt, key=None)
# st.markdown('- 文本输入')
# title = st.text_input('Movie title', a)
# st.write(title)
# st.write('The current movie title is', title)
#
# txt = st.text_area('Text to analyze', '''
#     It was the best of times, it was the worst of times, it was
#     the age of wisdom, it was the age of foolishness, it was
#     the epoch of belief, it was the epoch of incredulity, it
#     was the season of Light, it was the season of Darkness, it
#     was the spring of hope, it was the winter of despair, (...)
#     ''')

    # st.write('Sentiment:', run_sentiment_analysis(return_txt))

if work_in_button:
    st.write(work_chose)
if add_button1:
    machine()
# add_button2=st.sidebar.button('按业务')
# 主栏
