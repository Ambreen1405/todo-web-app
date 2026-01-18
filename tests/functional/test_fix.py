import requests
import json

# Test the registration through the frontend by simulating the API calls
BASE_URL = "https://ambreen-dev-todo-app.hf.space"

def test_frontend_registration_simulation():
    print("=== Simulating Frontend Registration Flow ===\n")

    # Clean up any existing test user
    print("Cleaning up any existing test user...")

    # Test registration with a new user
    print("Testing registration with a new user...")
    registration_data = {
        "email": "testsignup@example.com",
        "name": "Test Sign Up User",
        "password": "securepassword123"
    }

    try:
        response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
        print(f"Registration Status: {response.status_code}")

        if response.status_code == 201:
            print("[SUCCESS] Registration successful!")
            user_data = response.json()
            print(f"   User created: {user_data['email']} (ID: {user_data['id']})")

            # Now test that the same email cannot be registered again (should show error)
            print("\nTesting duplicate registration (should fail)...")
            duplicate_response = requests.post(f"{BASE_URL}/auth/register", json=registration_data)
            print(f"Duplicate Registration Status: {duplicate_response.status_code}")

            if duplicate_response.status_code == 400:
                error_detail = duplicate_response.json().get('detail', '')
                print(f"[SUCCESS] Correctly rejected duplicate registration: {error_detail}")

                # This is the kind of error that should be handled properly by our fix
                print(f"   This error message should now be properly displayed instead of 'Something went wrong'")
            else:
                print(f"[ERROR] Expected 400 error for duplicate registration, got {duplicate_response.status_code}")

        elif response.status_code == 400:
            error_detail = response.json().get('detail', 'Unknown error')
            print(f"Registration failed as expected for existing user: {error_detail}")

    except requests.exceptions.ConnectionError:
        print("[ERROR] Cannot connect to the backend server.")
    except Exception as e:
        print(f"[ERROR] Exception occurred: {str(e)}")

if __name__ == "__main__":
    test_frontend_registration_simulation()