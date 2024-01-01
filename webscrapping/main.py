from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
import pandas as pd
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import time
# Set up Chrome options for headless browsing
options = Options()
options.headless = True

# Initialize the Chrome driver
service = Service(executable_path="chromedriver")
driver = webdriver.Chrome(service=service, options=options)

# URL of the website
url = "https://dataviz.theanalyst.com/opta-power-rankings/"

# Navigate to the URL
driver.get(url)

# List to hold all DataFrames
dataframes = []

# Number of times you need to click the 'Continue' button
num_clicks = 20  # Adjust this number based on your needs

for _ in range(num_clicks):
    # Wait for the table to be visible and extract its data
    time.sleep(5)
    wait = WebDriverWait(driver, 10)
    table_class = "rTj3JAZb677k7iyR_pMD"
    wait.until(EC.visibility_of_element_located((By.CLASS_NAME, table_class)))
    table = driver.find_element(By.CLASS_NAME, table_class)
    table_html = table.get_attribute('outerHTML')
    df = pd.read_html(table_html)[0]
    dataframes.append(df)

    # Click the 'Continue' button to load the next table
    continue_button_xpath = "/html/body/div/div/div/div[2]/div[8]/div[2]/button[2]"
    continue_button = wait.until(EC.element_to_be_clickable((By.XPATH, continue_button_xpath)))
    continue_button.click()

# Closing the driver
driver.quit()

# Combine all DataFrames if needed
combined_df = pd.concat(dataframes, ignore_index=True)

# Display the combined dataframe
print(combined_df)

combined_df.to_csv('output.csv', index=False)