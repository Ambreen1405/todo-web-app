import requests
import json

# Final test to verify the signup functionality is working
BASE_URL = "https://ambreen-dev-todo-app.hf.space"

def final_test():
    print("=== Final Test of Registration Fix ===\n")

    # Test registration with a completely new user
    print("Testing registration with a new user...")
    registration_data = {
        "email": "finaltest@example.com",
        "name": "Final Test User",
        "password": "securepassword123"
    }

    try:
        # First, try to register the new user
        response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
        print(f"Registration Status: {response.status_code}")

        if response.status_code == 201:
            print("[SUCCESS] New user registration successful!")
            user_data = response.json()
            print(f"   User created: {user_data['email']} (ID: {user_data['id']})")

            # Test login with the newly created user
            print("\nTesting login with the newly created user...")
            login_data = {
                "email": "finaltest@example.com",
                "password": "securepassword123"
            }

            login_response = requests.post(f"{BASE_URL}/auth/login", json=login_data)
            print(f"Login Status: {login_response.status_code}")

            if login_response.status_code == 200:
                login_result = login_response.json()
                print("[SUCCESS] Login successful!")
                print(f"   Token received (length: {len(login_result.get('access_token', ''))})")

                # Test protected endpoint
                headers = {"Authorization": f"Bearer {login_result['access_token']}"}
                me_response = requests.get(f"{BASE_URL}/auth/me", headers=headers)
                print(f"Protected endpoint Status: {me_response.status_code}")

                if me_response.status_code == 200:
                    user_info = me_response.json()
                    print(f"[SUCCESS] Token verification successful! User: {user_info['email']}")

            else:
                print(f"[ERROR] Login failed: {login_response.text}")

        elif response.status_code == 400:
            error_detail = response.json().get('detail', 'Unknown error')
            print(f"[INFO] User already existed: {error_detail}")

            # Still test the login functionality
            print("\nTesting login with existing user...")
            login_data = {
                "email": "finaltest@example.com",
                "password": "securepassword123"
            }

            login_response = requests.post(f"{BASE_URL}/auth/login", json=login_data)
            print(f"Login Status: {login_response.status_code}")

            if login_response.status_code == 200:
                login_result = login_response.json()
                print("[SUCCESS] Login successful!")
                print(f"   Token received (length: {len(login_result.get('access_token', ''))})")
            else:
                print(f"[ERROR] Login failed: {login_response.text}")

        # Test error scenario - wrong password
        print("\nTesting login with wrong password (should fail)...")
        wrong_login_data = {
            "email": "finaltest@example.com",
            "password": "wrongpassword"
        }

        wrong_login_response = requests.post(f"{BASE_URL}/auth/login", json=wrong_login_data)
        print(f"Wrong password login Status: {wrong_login_response.status_code}")

        if wrong_login_response.status_code == 401:
            print("[SUCCESS] Correctly rejected wrong password!")
        else:
            print(f"[ERROR] Should have failed with wrong password: {wrong_login_response.text}")

    except requests.exceptions.ConnectionError:
        print("[ERROR] Cannot connect to the backend server.")
    except Exception as e:
        print(f"[ERROR] Exception occurred: {str(e)}")

if __name__ == "__main__":
    final_test()