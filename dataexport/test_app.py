#!/usr/bin/env python3
"""
Test script for the Flask Data Export Application
"""

import requests
import time

BASE_URL = "http://localhost:5001"
PASSWORD = "secure_password_123"
TEST_EMAIL = "test@example.com"

def test_application():
    """Test the application workflow"""
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    print("Testing Flask Data Export Application")
    print("-" * 50)
    
    # Test 1: Check redirect to login
    print("1. Testing home page redirects to login...")
    response = session.get(f"{BASE_URL}/")
    assert response.status_code == 200  # After redirect
    assert "login" in response.url.lower()
    print("   ✓ Correctly redirects to login page")
    
    # Test 2: Login with correct password
    print("2. Testing login with correct password...")
    login_data = {"password": PASSWORD}
    response = session.post(f"{BASE_URL}/login", data=login_data, allow_redirects=True)
    assert response.status_code == 200
    assert "download" in response.url.lower() or "Export Data" in response.text
    print("   ✓ Successfully logged in")
    
    # Test 3: Access download form
    print("3. Testing access to download form...")
    response = session.get(f"{BASE_URL}/download")
    assert response.status_code == 200
    assert "email" in response.text.lower()
    print("   ✓ Download form accessible")
    
    # Test 4: Get list of tables via API
    print("4. Testing API endpoint for table list...")
    response = session.get(f"{BASE_URL}/api/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert len(data["tables"]) > 0
    print(f"   ✓ API returned {data['count']} tables")
    
    # Test 5: Test logout
    print("5. Testing logout...")
    response = session.get(f"{BASE_URL}/logout")
    assert response.status_code == 200
    assert "login" in response.url.lower()
    print("   ✓ Successfully logged out")
    
    # Test 6: Verify protected routes require login
    print("6. Testing protected routes require authentication...")
    response = session.get(f"{BASE_URL}/download")
    assert response.status_code == 200
    assert "login" in response.url.lower()
    print("   ✓ Protected routes correctly require login")
    
    print("\n" + "=" * 50)
    print("All tests passed successfully!")
    print("\nApplication is ready for use at:")
    print(f"  {BASE_URL}")
    print(f"\nDefault credentials:")
    print(f"  Password: {PASSWORD}")
    
    return True

if __name__ == "__main__":
    try:
        test_application()
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        exit(1)
    except requests.exceptions.ConnectionError:
        print("\n✗ Could not connect to Flask application")
        print("  Make sure the application is running on port 5001")
        exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        exit(1)