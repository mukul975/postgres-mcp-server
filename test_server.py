"""
Test script for PostgreSQL MCP Server

This script demonstrates how to test the server locally without requiring
a full MCP client setup.
"""

import asyncio
import os
from postgres_server import (
    PostgreSQL_list_tables, 
    PostgreSQL_describe_table, 
    execute_query,
    TableInfo,
    ColumnInfo
)

async def test_connection():
    """Test basic database connectivity"""
    try:
        # Test a simple query to verify connection
        result = await execute_query("SELECT version()")
        print("✅ Database connection successful!")
        print(f"PostgreSQL version: {result[0]['version'] if result else 'Unknown'}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print(f"DATABASE_URL: {os.getenv('DATABASE_URL', 'Not set')}")
        return False

async def test_list_tables():
    """Test listing tables in public schema"""
    try:
        tables = await PostgreSQL_list_tables("public")
        print(f"✅ Found {len(tables)} tables in 'public' schema")
        for table in tables[:5]:  # Show first 5 tables
            print(f"  - {table.table_name} ({table.table_type})")
        if len(tables) > 5:
            print(f"  ... and {len(tables) - 5} more")
        return True
    except Exception as e:
        print(f"❌ Failed to list tables: {e}")
        return False

async def test_information_schema():
    """Test accessing information_schema"""
    try:
        # Try to access information_schema tables
        result = await execute_query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'information_schema' LIMIT 5"
        )
        print(f"✅ Can access information_schema ({len(result)} tables found)")
        return True
    except Exception as e:
        print(f"❌ Cannot access information_schema: {e}")
        return False

async def run_tests():
    """Run all tests"""
    print("🧪 Testing PostgreSQL MCP Server...")
    print("=" * 50)
    
    # Check if DATABASE_URL is set
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("❌ DATABASE_URL environment variable not set")
        print("Please set it with: export DATABASE_URL='postgresql://username:password@host:port/database'")
        return
    
    print(f"🔗 Connecting to: {db_url.split('@')[1] if '@' in db_url else 'Unknown host'}")
    print()
    
    tests = [
        ("Database Connection", test_connection),
        ("Information Schema Access", test_information_schema),
        ("List Tables", test_list_tables),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"🧪 Running: {test_name}")
        try:
            if await test_func():
                passed += 1
            print()
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            print()
    
    print("=" * 50)
    print(f"📊 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Your PostgreSQL MCP server is ready to use.")
        print("\nNext steps:")
        print("1. Run the server: python postgres_server.py")
        print("2. Or test with MCP tools: mcp dev postgres_server.py")
    else:
        print("⚠️  Some tests failed. Please check your PostgreSQL configuration.")

if __name__ == "__main__":
    # Try to load from .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("💡 Tip: Install python-dotenv to automatically load .env files")
        print("   pip install python-dotenv")
    
    asyncio.run(run_tests())
