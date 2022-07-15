from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time
import requests
from selenium.webdriver.chrome.options import Options
from zipfile import ZipFile
import os
import pymysql
import glob
import pandas as pd
from sqlalchemy import create_engine
global driver
global name

def build_driver(): # 建立webdriver
    opts = Options()
    # opts.add_argument("--headless") 
    opts.add_argument("--disable-notifications") 
    #設定下載位置
    prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': 'C:\\Users\\zxc85\\OneDrive\\桌面\\面試考題\\資料\\'}
    opts.add_experimental_option('prefs', prefs)
    # 啟動ChromeDriver
    driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), chrome_options=opts)
    driver.get("https://plvr.land.moi.gov.tw/DownloadOpenData")

    driver.find_element(By.XPATH, '//*[@id="tab_opendata"]/ul/li[2]').click()
    driver.find_element(By.XPATH, '//*[@id="historySeason_id"]').send_keys("108年第2季")
    driver.find_element(By.XPATH, '//*[@id="fileFormatId"]').send_keys("CSV")
    driver.find_element(By.XPATH, '//*[@id="downloadTypeId2"]').click()
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    city = soup.findAll('tr', class_='advDownloadClass')
    citydemo = "臺北市/新北市/桃園市/臺中市/高雄市".split("/")
    s = 6-1
    name ={}
    # print(city[1])
    for i in city:
        if i.find('td').find('font').text in citydemo:
            cityname = i.find('td').find('font').text
            cityvalue = i.findAll('td')[1].find('input')['value']
            name[cityvalue] = cityname
            driver.find_element(By.XPATH, f'//*[@id="table5"]/tbody/tr[{s}]/td[2]/input').click()
        s += 1
    driver.find_element(By.XPATH, '//*[@id="downloadBtnId"]').click()

def cheack_zip():  
    while True:
        non_domestic = glob.glob(os.path.join('*.zip'))
        if len(non_domestic) == 1:
            return True

def unzip(zip):
    with ZipFile(zip, 'r') as obj_zip:
        FileNames = obj_zip.namelist()
        for i in FileNames:
            if i.split('.')[0] in name:
                obj_zip.extract(i, '資料')
                obj_zip.close()                
                break

def db_init1():
    db = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='root',
        port=3306,
    )
    cursor = db.cursor(pymysql.cursors.DictCursor)
    return db, cursor

def create_city():
    db, cursor = db_init1()
    sql = f"""
        create database if not exists city;
        """
    cursor.execute(sql)
    db.commit()  # 測試,將執行成功的結果存進database裡
    db.close()
    print('success')

def sql_init():
    engine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/city')
    path_domestic = os.path.abspath(os.getcwd()) + '/資料'
    non_domestic = glob.glob(os.path.join(path_domestic,'*.csv'))
    print(non_domestic)
    for i in non_domestic:   
        df = pd.read_csv(i, sep=',' ,encoding= 'utf-8-sig')
        # 將新建的DataFrame儲存為MySQL中的資料表，不儲存index列
        df[1:].to_sql('city', engine, if_exists='append', index=False)  


if __name__ == "__main__":
    build_driver() # 建立webdriver
    cheack_zip() # 檢查是否有zip檔
    driver.quit() # 關閉webdriver
    unzip("download.zip") # 解壓縮
    create_city() # 建立資料庫
    sql_init() # 將資料存進資料庫