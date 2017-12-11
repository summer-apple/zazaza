from selenium import webdriver


driver = webdriver.PhantomJS()
driver.set_page_load_timeout(30)
driver.maximize_window()
driver.get('https://scontent-lax3-2.cdninstagram.com/t51.2885-15/e35/23824577_718372778352303_4830831136953335808_n.jpg')
print(driver.title)
with open('ccc.jpg','w') as f:
    f.write(driver.page_source)
source = driver.page_source

driver.save_screenshot('bbb.png')