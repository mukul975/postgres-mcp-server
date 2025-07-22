#!/usr/bin/env python3
import asyncio
import asyncpg

async def test_connection():
    """Test PostgreSQL connection with the URL-encoded password."""
    
    # Connection string with URL-encoded password (@ becomes %40)
    connection_string = "postgresql://postgres:Mukul%40975@localhost:5432/postgres"
    
    try:
        print("Testing PostgreSQL connection...")
        connection = await asyncpg.connect(connection_string)
        print("✅ Connection successful!")
        
        # Test a simple query
        result = await connection.fetchval("SELECT version()")
        print(f"PostgreSQL version: {result}")
        
        # List some basic info
        tables = await connection.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        print(f"Found {len(tables)} tables in public schema:")
        for table in tables[:5]:  # Show first 5 tables
            print(f"  - {table['table_name']}")
        
        await connection.close()
        print("✅ Connection closed successfully")
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = asyncio.run(test_connection())
    if success:
        print("\n🎉 PostgreSQL connection test passed! Ready to implement MCP tools.")
    else:
        print("\n💥 Connection test failed. Please check your PostgreSQL setup.")
