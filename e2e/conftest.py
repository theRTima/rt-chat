import pytest
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


APP_URL = "http://localhost:3000"


def _make_driver(user_id, username):
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,720")
    opts.add_argument("--disable-gpu")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts,
    )

    # Inject localStorage before any page JS runs so React picks up our IDs
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": f"""
            localStorage.setItem('userId', '{user_id}');
            localStorage.setItem('username', '{username}');
        """
    })
    driver.get(APP_URL)
    return driver


@pytest.fixture
def browser_alice():
    driver = _make_driver("e2e_alice", "Alice")
    yield driver
    driver.quit()


@pytest.fixture
def browser_bob():
    driver = _make_driver("e2e_bob", "Bob")
    yield driver
    driver.quit()
