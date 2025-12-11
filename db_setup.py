#!/usr/bin/env python3
"""
Database management script for mentor-scoring application
Usage:
  python db_setup.py init     - Initialize database
  python db_setup.py seed     - Seed default users
  python db_setup.py list     - List all users
  python db_setup.py delete   - Delete all users (CAREFUL!)
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from models import User, init_db, seed_default_users, users_collection


def list_users():
    """List all users in the database"""
    print("\n" + "="*60)
    print("All Users in Database")
    print("="*60)
    
    users = User.get_all_users()
    
    if not users:
        print("No users found in database.")
        return
    
    for i, user in enumerate(users, 1):
        print(f"\n{i}. {user['name']}")
        print(f"   Email: {user['email']}")
        print(f"   Role: {user['role']}")
        print(f"   Active: {user.get('is_active', True)}")
        print(f"   Created: {user.get('created_at', 'N/A')}")


def delete_all_users():
    """Delete all users from the database (CAREFUL!)"""
    print("\n⚠️  WARNING: This will delete ALL users from the database!")
    response = input("Type 'YES' to confirm: ").strip()
    
    if response != 'YES':
        print("Deletion cancelled.")
        return
    
    result = users_collection.delete_many({})
    print(f"✓ Deleted {result.deleted_count} users")


def init_and_seed():
    """Initialize database and seed default users"""
    print("\nInitializing database...")
    try:
        init_db()
        print("✓ Database indexes created")
    except Exception as e:
        print(f"⚠ Database already initialized or error: {e}")
    
    print("\nSeeding default users...")
    try:
        seed_default_users()
        print("✓ Default users seeded")
    except Exception as e:
        print(f"⚠ Seeding error: {e}")


def test_login():
    """Test login with all three users"""
    print("\n" + "="*60)
    print("Testing Login with Default Users")
    print("="*60)
    
    test_users = [
        {'email': 'student@example.com', 'password': 'student123', 'role': 'student'},
        {'email': 'mentor@example.com', 'password': 'mentor123', 'role': 'mentor'},
        {'email': 'university@example.com', 'password': 'university123', 'role': 'university'},
    ]
    
    for test_user in test_users:
        print(f"\nTesting: {test_user['email']} ({test_user['role']})")
        user = User.verify_password(test_user['email'], test_user['password'])
        if user:
            print(f"  ✓ Login successful!")
            print(f"    Name: {user['name']}")
            print(f"    ID: {user['_id']}")
        else:
            print(f"  ✗ Login failed!")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == 'init':
        init_and_seed()
    elif command == 'seed':
        seed_default_users()
    elif command == 'list':
        list_users()
    elif command == 'delete':
        delete_all_users()
    elif command == 'test':
        test_login()
    elif command == 'test-login':
        test_login()
    else:
        print(f"Unknown command: {command}")
        print(__doc__)
        sys.exit(1)
