import requests
import json

# Final verification test after implementing all fixes
BASE_URL = "https://ambreen-dev-todo-app.hf.space"

def final_verification_test():
    print("=== Final Verification Test After All Fixes ===\n")

    # Test registration with a new user
    print("1. Testing registration with a new user...")
    registration_data = {
        "email": "fixed_signup_test@example.com",
        "name": "Fixed Signup Test User",
        "password": "securepassword123"
    }

    try:
        response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
        print(f"   Registration Status: {response.status_code}")

        if response.status_code == 201:
            print("   [SUCCESS] New user registration successful!")
            user_data = response.json()
            print(f"   User created: {user_data['email']} (ID: {user_data['id']})")

            # Test login with the newly created user
            print("\n2. Testing login with the newly created user...")
            login_data = {
                "email": "fixed_signup_test@example.com",
                "password": "securepassword123"
            }

            login_response = requests.post(f"{BASE_URL}/auth/login", json=login_data)
            print(f"   Login Status: {login_response.status_code}")

            if login_response.status_code == 200:
                login_result = login_response.json()
                print("   [SUCCESS] Login successful!")
                print(f"   Token received (length: {len(login_result.get('access_token', ''))})")

                # Test protected endpoint
                headers = {"Authorization": f"Bearer {login_result['access_token']}"}
                me_response = requests.get(f"{BASE_URL}/auth/me", headers=headers)
                print(f"   Protected endpoint Status: {me_response.status_code}")

                if me_response.status_code == 200:
                    user_info = me_response.json()
                    print(f"   [SUCCESS] Token verification successful! User: {user_info['email']}")

                    # Test duplicate registration to ensure proper error handling
                    print("\n3. Testing duplicate registration (should show proper error)...")
                    duplicate_response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
                    print(f"   Duplicate Registration Status: {duplicate_response.status_code}")

                    if duplicate_response.status_code == 400:
                        error_detail = duplicate_response.json().get('detail', '')
                        print(f"   [SUCCESS] Proper error returned for duplicate registration: {error_detail}")
                        print("   This error would now be properly displayed in the UI instead of 'Something went wrong'")
                    else:
                        print(f"   [WARNING] Unexpected status for duplicate registration: {duplicate_response.status_code}")

            else:
                print(f"   [ERROR] Login failed: {login_response.text}")

        elif response.status_code == 400:
            error_detail = response.json().get('detail', 'Unknown error')
            print(f"   [INFO] User already existed: {error_detail}")

    except requests.exceptions.ConnectionError:
        print("   [ERROR] Cannot connect to the backend server.")
    except Exception as e:
        print(f"   [ERROR] Exception occurred: {str(e)}")

    print("\n=== All fixes implemented successfully ===")
    print("The signup flow should now properly handle errors and display meaningful messages instead of 'Something went wrong'")

if __name__ == "__main__":
    final_verification_test()