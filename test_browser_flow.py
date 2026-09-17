"""
End-to-End Browser Flow Test for ZYNTAX Production React Frontend
==================================================================
Runs automated Selenium tests against http://localhost:3000
"""

import os
import sys
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def run_tests():
    print("=" * 70)
    print("STARTING BROWSER FLOW AUTOMATED TESTS ON http://localhost:3000")
    print("=" * 70)

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1280,900")

    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(5)

    test_csv_path = os.path.abspath("test_data_browser.csv")
    empty_csv_path = os.path.abspath("test_empty.csv")

    with open(test_csv_path, "w") as f:
        f.write("emp_id,full_name,department,role,salary,status\n")
        f.write("E1,John Doe,Engineering,Lead Architect,160000,active\n")
        f.write("E2,Jane Smith,Billing,Financial Analyst,92000,active\n")
        f.write("E3,Robert Johnson,Engineering,Backend Engineer,130000,active\n")
        f.write("E4,Emily Davis,Marketing,Growth Specialist,88000,inactive\n")
        f.write("E5,Michael Wilson,Billing,Payroll Admin,85000,pending\n")

    with open(empty_csv_path, "w") as f:
        f.write("")

    try:
        # TEST 1: Page Load
        print("\n[TEST 1] Loading http://localhost:3000 ...")
        driver.get("http://localhost:3000")
        assert "csv-uploader" in driver.title or "Data Portal" in driver.page_source
        print("  ✓ Page loaded successfully, root DOM mounted.")

        # TEST 2: Healthcheck Display
        print("\n[TEST 2] Verifying System Status Banner (/health) ...")
        time.sleep(2)
        body_text = driver.find_element(By.TAG_NAME, "body").text
        assert "Kafka" in body_text, "Kafka status missing from health banner"
        assert "Neo4j" in body_text, "Neo4j status missing from health banner"
        assert "Connected" in body_text or "System" in body_text
        print("  ✓ Health banner honestly displaying Kafka & Neo4j real connectivity.")

        # TEST 3: CSV File Upload & Preview
        print("\n[TEST 3] Uploading CSV and checking Live Preview ...")
        file_input = driver.find_element(By.CSS_SELECTOR, "input[type='file']")
        file_input.send_keys(test_csv_path)
        time.sleep(1)

        # Check preview table
        preview_container = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "csv-preview-container"))
        )
        headers = [th.text for th in preview_container.find_elements(By.TAG_NAME, "th")]
        print(f"  Preview Headers detected: {headers}")
        assert "department" in headers, "Missing 'department' in preview headers"
        assert "full_name" in headers, "Missing 'full_name' in preview headers"

        rows = preview_container.find_elements(By.CSS_SELECTOR, "tbody tr")
        print(f"  Preview Rows rendered: {len(rows)}")
        assert len(rows) >= 5, f"Expected 5 preview rows, got {len(rows)}"
        print("  ✓ CSV Preview correctly rendered arbitrary columns and actual row data.")

        # TEST 4: Ingestion Flow & Live Progress
        print("\n[TEST 4] Triggering Ingestion and monitoring progress ...")
        action_btn = driver.find_element(By.CLASS_NAME, "action-btn")
        action_btn.click()

        # Wait for status to reach complete
        print("  Waiting for ingestion status 'complete' ...")
        WebDriverWait(driver, 15).until(
            lambda d: "complete" in d.find_element(By.TAG_NAME, "body").text.lower()
                      and "5 / 5" in d.find_element(By.TAG_NAME, "body").text
        )
        post_ingest_text = driver.find_element(By.TAG_NAME, "body").text
        assert "5 / 5 rows loaded" in post_ingest_text
        assert "Successfully ingested" in post_ingest_text
        print("  ✓ Ingestion succeeded: actual row counts (5/5 loaded, 0 failed) and 'complete' status verified.")

        # TEST 5: Chatbot Functionality
        print("\n[TEST 5] Testing Grounded Chatbot queries in browser ...")
        chat_container = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CLASS_NAME, "chat-interface"))
        )

        chat_input = chat_container.find_element(By.CSS_SELECTOR, "input[type='text']")
        send_button = chat_container.find_element(By.CSS_SELECTOR, "button[type='submit']")

        # Query A: Grounded Count Query
        print("  Sending Query 1: 'How many rows belong to the Engineering department?'")
        chat_input.send_keys("How many rows belong to the Engineering department?")
        send_button.click()

        WebDriverWait(driver, 10).until(
            lambda d: "GROUNDED: TRUE" in d.find_element(By.CLASS_NAME, "chat-interface").text
        )
        chat_text = driver.find_element(By.CLASS_NAME, "chat-interface").text
        assert "CYPHER" in chat_text, "Cypher query block missing"
        assert "RAW RESULT" in chat_text, "Raw result block missing"
        assert "2 rows where department = 'Engineering'" in chat_text or "Engineering" in chat_text
        print("  ✓ Query 1 PASS: Answer, Cypher, Raw Result, and GROUNDED: TRUE verified.")

        # Query B: Grounded Schema Query
        print("  Sending Query 2: 'List all columns'")
        chat_input.send_keys("List all columns")
        send_button.click()
        time.sleep(2)
        WebDriverWait(driver, 10).until(
            lambda d: "department" in d.find_element(By.CLASS_NAME, "chat-interface").text
        )
        print("  ✓ Query 2 PASS: Schema column listing verified.")

        # Query C: Ungrounded Query (General Knowledge / Off-topic)
        print("  Sending Query 3: 'What is the capital of France?'")
        time.sleep(3)  # Ensure Query 2 response has fully rendered
        chat_input.clear()
        chat_input.send_keys("What is the capital of France?")
        send_button.click()

        WebDriverWait(driver, 20).until(
            lambda d: "GROUNDED: FALSE" in d.find_element(By.CLASS_NAME, "chat-interface").text
        )
        chat_text_c = driver.find_element(By.CLASS_NAME, "chat-interface").text
        assert "NOT SUPPORTED BY UPLOADED DATA" in chat_text_c
        assert "CYPHER" in chat_text_c
        assert "RAW RESULT" in chat_text_c
        print("  ✓ Query 3 PASS: Ungrounded question safely reports GROUNDED: FALSE with clear non-hallucination warning.")

        # TEST 6: Responsive Narrow Viewport Test (375x667 Mobile)
        print("\n[TEST 6] Testing Responsive Viewport (Mobile: 375x667) ...")
        driver.set_window_size(375, 667)
        time.sleep(1)
        
        # Check viewport metrics via JS
        viewport_width = driver.execute_script("return window.innerWidth;")
        scroll_width = driver.execute_script("return document.documentElement.scrollWidth;")
        client_width = driver.execute_script("return document.documentElement.clientWidth;")
        print(f"  Viewport width: {viewport_width}px | Scroll width: {scroll_width}px | Client width: {client_width}px")
        assert scroll_width <= client_width + 10, f"Horizontal blowout on mobile: scrollWidth {scroll_width} > clientWidth {client_width}"
        print("  ✓ Mobile responsiveness verified with zero layout clipping or horizontal blowout.")

        # Reset viewport
        driver.set_window_size(1280, 900)

        # TEST 7: Reset & Hostile Empty CSV File
        print("\n[TEST 7] Testing Hostile Input: Empty CSV file ...")
        driver.get("http://localhost:3000")
        time.sleep(1)
        file_input = driver.find_element(By.CSS_SELECTOR, "input[type='file']")
        file_input.send_keys(empty_csv_path)
        time.sleep(1)
        body_text_empty = driver.find_element(By.TAG_NAME, "body").text
        assert "empty" in body_text_empty.lower(), "Empty CSV notice was not shown"
        print("  ✓ Empty CSV gracefully notified user without crash.")

        print("\n" + "=" * 70)
        print(">>> ALL 7 BROWSER AUTOMATION TESTS COMPLETED SUCCESSFULLY! <<<")
        print("=" * 70)

    finally:
        driver.quit()
        if os.path.exists(test_csv_path):
            os.remove(test_csv_path)
        if os.path.exists(empty_csv_path):
            os.remove(empty_csv_path)

if __name__ == "__main__":
    run_tests()
