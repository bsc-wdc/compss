#!/usr/bin/python3

import os, signal, time

from subprocess import run, Popen, PIPE, STDOUT
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from notebook import notebookapp
from selenium.webdriver.firefox.options import Options

options = Options()
options.headless = True
driver = webdriver.Firefox(options=options)

run('pycompss environment change default', shell=True)
try:

    jupyter_process = Popen('pycompss jupyter --no-browser ./src', shell=True, preexec_fn=os.setsid)
    time.sleep(3)
    while not list(notebookapp.list_running_servers()):
        pass

    server = list(notebookapp.list_running_servers())[0]

    url = server['url'] + f"?token={server['token']}"

    driver.get(url)
    notebook_css_selector = 'div.list_item:nth-child(2) > div:nth-child(1) > a:nth-child(3) > span:nth-child(1)'
    WebDriverWait(driver, 10).until(lambda driver: driver.find_element_by_css_selector(notebook_css_selector).is_enabled())
    driver.find_element_by_css_selector(notebook_css_selector).click()

    time.sleep(4)
    driver.switch_to.window(driver.window_handles[1])
    driver.find_element_by_css_selector('#celllink').click()
    driver.find_element_by_css_selector('#run_all_cells > a').click()


    WebDriverWait(driver, 60*3).until(lambda driver: driver.find_element_by_css_selector('#notebook-container > div.cell.code_cell.rendered.unselected > div.input > div.prompt_container > div.prompt.input_prompt').get_attribute("innerHTML") == "<bdi>In</bdi>&nbsp;[1]:")

    output = driver.find_element_by_css_selector('.output_subarea > pre:nth-child(1)').get_attribute("innerHTML")
    expected_lines = [
        '* - PyCOMPSs Runtime started... Have fun!              *',
        '*************** STOPPING PyCOMPSs ******************',
        'Results after stopping PyCOMPSs:',
        'a: 4',
        'b: 8',
        'c: 41'
    ]

    for el in expected_lines:
        if el not in output:
            print('ERROR: ' + el + ' not found')
            exit(1)

    print('OK')
finally:
    os.killpg(os.getpgid(jupyter_process.pid), signal.SIGTERM)
    driver.quit()