import unittest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
import random

import time

class Taobao():

    def __init__(self):
        self.driver = webdriver.Chrome()
        self.driver.set_window_size(1920,1080)
        self.sum = 0



    def open_page(self, url):
        self.driver.get(url)

    def login(self):

        self.driver.get('https://login.taobao.com')
        login_links = self.driver.find_element_by_class_name('J_Quick2Static')

        login_links.click()
        time.sleep(3.435)
        self.driver.find_element_by_id('TPL_username_1').send_keys('00000000')
        time.sleep(2.323)
        self.driver.find_element_by_id('TPL_password_1').send_keys('00000000')

        self.driver.find_element_by_id('J_SubmitStatic').click()
        time.sleep(10)
        self.driver.get('https://buyertrade.taobao.com/trade/itemlist/list_bought_items.htm')

    def all_order(self):

        while True:
            self.parse_page()

            next_page_btn = self.driver.find_element_by_css_selector('.pagination-next')
            cls = next_page_btn.get_attribute('class').split(' ')
            if 'pagination-disabled' in cls:
                break
            self.driver.execute_script('arguments[0].scrollIntoView(false);',next_page_btn)
            #self.driver.execute_script('window.scrollTo(0, document.body.scrollHeight)')
            time.sleep(3)
            next_page_btn.click()




    def parse_page(self):

        self.driver.implicitly_wait(30)
        order_list = self.driver.find_elements_by_css_selector('.js-order-container tbody tr td:nth-child(5) .price-mod__price___1BVLR > p > strong > span:nth-child(2)')

        for o in order_list:
            print(o.text)
            self.sum = self.sum + float(o.text)

        print('current sum:%s' % self.sum)
        time.sleep(random.random()*10)




    def _jump_to(self,page_num):
        page_input = self.driver.find_element_by_css_selector('.pagination-options-quick-jumper input')
        page_input.clear()
        page_input.send_keys(page_num)
        self.driver.find_element_by_class_name('pagination-options-go').click()
        time.sleep(random.random()*10)



    def close(self):
        print('至今已给马云送去',self.sum,'元！')
        time.sleep(500)
        self.driver.close()




if __name__ == "__main__":
    tb = Taobao()
    #tb.open_page('file:///home/summer/%E6%88%91%E7%9A%84%E6%B7%98%E5%AE%9D.html')
    # tb.open_page('file:///home/summer/%E5%B7%B2%E4%B9%B0%E5%88%B0%E7%9A%84%E5%AE%9D%E8%B4%9D.html')
    #
    # nt = tb.driver.find_element_by_css_selector('.pagination-next')
    # print(nt.get_attribute('class'))
    tb.login()
    tb.all_order()
    tb.close()
