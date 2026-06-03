import time

import pytest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


class TestChatE2E:
    """End-to-end tests for the chat application."""

    def test_alice_message_received_by_bob(self, browser_alice, browser_bob):
        """Two users join the same room; Alice sends a message; Bob receives it."""
        wait_a = WebDriverWait(browser_alice, 10)
        wait_b = WebDriverWait(browser_bob, 10)

        # --- Wait for both WebSocket connections to be established ---
        wait_a.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".connection-status.connected")
            )
        )
        wait_b.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".connection-status.connected")
            )
        )

        # Both browsers auto-join "general" on connect.
        # Give the hub a moment to process joins and broadcast user_joined
        # notifications so the message input becomes enabled.
        time.sleep(0.5)

        # --- Alice types and sends a message ---
        msg_text = f"Hello from Alice! {time.time():.6f}"

        input_a = wait_a.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".message-input-field"))
        )
        input_a.clear()
        input_a.send_keys(msg_text)

        # Wait for the send button to become enabled (React needs to re-render)
        send_btn = wait_a.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".send-button"))
        )
        send_btn.click()

        # --- Assert Bob receives the exact message ---
        msg_element = wait_b.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    f"//div[contains(@class, 'message-content') and contains(text(), '{msg_text}')]",
                )
            )
        )
        assert (
            msg_element.text == msg_text
        ), f"Expected '{msg_text}', got '{msg_element.text}'"

    @pytest.mark.parametrize("side", ["alice", "bob"])
    def test_own_message_appears_in_sender(self, browser_alice, browser_bob, side):
        """Verify the sender also sees their own message in the feed."""
        browser = browser_alice if side == "alice" else browser_bob
        wait = WebDriverWait(browser, 10)

        # Wait for connection
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, ".connection-status.connected")
            )
        )
        time.sleep(0.5)

        # Send a message
        msg_text = f"Own message from {side} {time.time():.6f}"
        inp = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".message-input-field"))
        )
        inp.clear()
        inp.send_keys(msg_text)
        wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".send-button"))
        ).click()

        # Wait for the message to appear in the feed
        content = wait.until(
            EC.presence_of_element_located(
                (
                    By.XPATH,
                    f"//div[contains(@class, 'message-content') and contains(text(), '{msg_text}')]",
                )
            )
        )
        assert content.text == msg_text

        # Verify the message has the own-message styling
        msg_div = content.find_element(By.XPATH, "./..")
        assert "own-message" in msg_div.get_attribute("class"), (
            f"Expected own-message class on {side}'s message"
        )
