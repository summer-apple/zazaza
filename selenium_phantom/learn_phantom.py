from selenium import webdriver


driver = webdriver.PhantomJS()
driver.set_page_load_timeout(30)
driver.maximize_window()
driver.get('http://www.huaban.com')
print(driver.title)
driver.save_screenshot('huaban.png')