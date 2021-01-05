#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 15:41:58 2021

@author: patrickyip
"""


import requests
import pymysql
from sqlalchemy import create_engine
import json
import pandas as pd
import time
from tqdm import tqdm

class JD_crawler():
    
    def __init__(self):
        
        self.engine = create_engine("mysql+pymysql://root:password@localhost:3306/JD_comments")
        
    
    def get_proxy(self):
        #获取proxy的方式
        
        return proxy
    
    def get_json(self,url,proxy):
        
        
        
        res = requests.get(
            url,
            headers ={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
            },
            proxies = proxy

        )
        
        try:
            res.raise_for_status()
            
        except e as Exception:
            print (e)
            
        content = res.content.decode('gbk')
        content = content.lstrip("fetchJSON_comment98(").rstrip(");")
        
        try:

            json_file = json.loads(content)
        
        except:
            
            return None
        
        return json_file
    
    def parse_json(self,content,product,brand):
        

        comment = content.get("comments")
        text_list = []
        time_list = []
        id_list = []
        score_list = []

        for line in comment:

            text_list.append(line.get("content"))
            time_list.append(line.get("creationTime"))
            id_list.append(line.get("id"))
            score_list.append(line.get("score"))


        df = pd.DataFrame({"content":text_list,"time":time_list,"id":id_list,"score":score_list})
        df["brand"] = brand
        df["product"] = product

        
        # 存数据库
        # 也可以选择写入文件，看需要
        df.to_sql('jd_comment_detail', self.engine,if_exists = "append")
            

        
        
    def main(self,url,productName,productBrand):
        retry_count = 0
        
        while retry_count <3:
            # 三次重试，选择性
            
            proxy = self.get_proxy()
            
            content =self.get_json(url,proxy)
            
            if content:
                
                self.parse_json(content,productName,productBrand)
                
                return True
            else:
#                 time.sleep(2)
                retry_count +=1

        return False
if __name__ == "__main__":
    
    product = pd.read_csv("./JD_item.csv").to_dict("record")
    
    
    for val in tqdm(product):

        productId = val["link"].split("/")[-1].split(".")[0]
        productName = val["product"]
        productBrand = val["brand"]
        print(f"ready to crawl {productName}")
        
        page = 1

        for i in tqdm(range(100)):
            
            url = f"https://club.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98&productId={productId}&score=0&sortType=5&page={page}&pageSize=10&isShadowSku=0&rid=0&fold=1"
            crawler = JD_crawler()
            signal = crawler.main(url,productName,productBrand)
            # signal 为True和False，False即为失败
            if not signal:
                break
            page +=1
            time.sleep(2)