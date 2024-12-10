from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Enable headless mode
chrome_options.add_argument("--disable-gpu")  # Disable GPU (optional but recommended for headless mode)
chrome_options.add_argument("--no-sandbox")  # For Linux systems
chrome_options.add_argument("--disable-extensions")  # Disable extensions (optional)

# Initialize the WebDriver with options
driver = webdriver.Chrome(options=chrome_options)

try:
    # Navigate to the URL
    url = "https://www.google.com"
    driver.get(url)

    # Get the page source
    print(driver.page_source)
finally:
    # Close the driver
    driver.quit()
