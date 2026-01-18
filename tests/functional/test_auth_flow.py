import requests
import json

# Test the complete authentication flow
BASE_URL = "https://ambreen-dev-todo-app.hf.space"

def test_auth_flow():
    print("=== Testing Authentication Flow ===\n")

    # Step 1: Test registration
    print("1. Testing registration endpoint...")
    registration_data = {
        "email": "auth_test@example.com",
        "name": "Auth Test User",
        "password": "securepassword123"
    }

    try:
        reg_response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
        print(f"   Registration Status: {reg_response.status_code}")

        if reg_response.status_code == 201:
            print("   [SUCCESS] User registered successfully!")
            reg_data = reg_response.json()
            print(f"   Registered User: {reg_data['email']} (ID: {reg_data['id']})")
        elif reg_response.status_code == 400 and "already exists" in reg_response.json().get('detail', ''):
            print("   [INFO] User already exists, continuing with login test...")
        else:
            print(f"   [ERROR] Registration failed: {reg_response.text}")
            return

        # Step 2: Test login
        print("\n2. Testing login endpoint...")
        login_data = {
            "email": "auth_test@example.com",
            "password": "securepassword123"
        }

        login_response = requests.post(f"{BASE_URL}/auth/login", json=login_data)
        print(f"   Login Status: {login_response.status_code}")

        if login_response.status_code == 200:
            login_result = login_response.json()
            access_token = login_result.get('access_token')
            print("   [SUCCESS] Login successful!")
            print(f"   Token Type: {login_result.get('token_type')}")
            print(f"   Access Token Length: {len(access_token) if access_token else 'None'}")

            # Step 3: Test accessing protected endpoint with the token
            print("\n3. Testing protected endpoint (/auth/me)...")
            headers = {"Authorization": f"Bearer {access_token}"}
            me_response = requests.get(f"{BASE_URL}/auth/me", headers=headers)
            print(f"   Protected endpoint Status: {me_response.status_code}")

            if me_response.status_code == 200:
                user_info = me_response.json()
                print("   [SUCCESS] Protected endpoint access successful!")
                print(f"   Current User: {user_info['email']} (ID: {user_info['id']})")

                # Verify that the token corresponds to the correct user
                if user_info['email'] == "auth_test@example.com":
                    print("   [VERIFICATION] Token correctly corresponds to logged-in user!")
                else:
                    print(f"   [ISSUE] Token returned user {user_info['email']} instead of auth_test@example.com!")
            else:
                print(f"   [ERROR] Failed to access protected endpoint: {me_response.text}")

        else:
            print(f"   [ERROR] Login failed: {login_response.text}")

    except requests.exceptions.ConnectionError:
        print("   [ERROR] Cannot connect to the backend server.")
    except Exception as e:
        print(f"   [ERROR] Exception occurred: {str(e)}")

if __name__ == "__main__":
    test_auth_flow()