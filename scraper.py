from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
import time


start_url = "https://www.expansion.com/mercados/cotizaciones/indices/ibex35_I.IB.html"

with webdriver.Firefox() as driver:
  
    wait = WebDriverWait(driver, 15)
    driver.get(start_url)

    time.sleep(1)

    #recuperamos listado de acciones
    commodities = driver.find_elements_by_xpath("/html/body/main/section/div/div/div/ul/li/div/section/div/article/section[2]/ul[2]/li[1]/div/section/table/tbody/tr")
    
    file = open("log_gecko.csv", "a")
    while 1:
        for stock in commodities:
            #recuperamos la información de cada acción
            values = stock.find_elements_by_xpath("td")        
            list_of_values = [x.text for x in values]
            linea = ""
            for value in range(len(list_of_values)-1):
                if value != (len(list_of_values)-2):
                    #eliminamos el punto de millar y cambiamos la coma decimal
                    v = list_of_values[value].replace('.','')
                    v = v.replace(',','.')
                    print(v,end=",")
                    linea += str(v)+","
                    
                else:
                    print(list_of_values[value])
                    
            currentDateAndTime = datetime.now()        
            file.write(linea + currentDateAndTime.strftime("%H:%M") + ",04/17" + "\n")
            commodities = driver.find_elements_by_xpath("/html/body/main/section/div/div/div/ul/li/div/section/div/article/section[2]/ul[2]/li[1]/div/section/table/tbody/tr")
        
