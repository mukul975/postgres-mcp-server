#!/usr/bin/env python3
"""
Setup script for PostgreSQL MCP Server

This script helps set up the development environment and dependencies.
"""

import os
import sys
import subprocess
import platform

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return None

def check_python_version():
    """Check if Python version is compatible."""
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not compatible. Requires Python 3.8+")
        return False

def setup_virtual_environment():
    """Set up virtual environment if it doesn't exist."""
    if os.path.exists('venv'):
        print("✅ Virtual environment already exists")
        return True
    
    return run_command("python -m venv venv", "Creating virtual environment")

def install_dependencies():
    """Install required dependencies."""
    activation_script = "venv\\Scripts\\activate" if platform.system() == "Windows" else "source venv/bin/activate"
    pip_command = "venv\\Scripts\\pip" if platform.system() == "Windows" else "venv/bin/pip"
    
    return run_command(f"{pip_command} install -r requirements.txt", "Installing dependencies")

def setup_env_file():
    """Set up .env file if it doesn't exist."""
    if os.path.exists('.env'):
        print("✅ .env file already exists")
        return True
    
    if os.path.exists('.env.example'):
        try:
            with open('.env.example', 'r') as src, open('.env', 'w') as dst:
                dst.write(src.read())
            print("✅ Created .env file from .env.example")
            print("⚠️  Please edit .env file with your actual database credentials")
            return True
        except Exception as e:
            print(f"❌ Failed to create .env file: {e}")
            return False
    else:
        print("❌ .env.example file not found")
        return False

def main():
    """Main setup function."""
    print("🚀 Setting up PostgreSQL MCP Server Development Environment")
    print("=" * 60)
    
    # Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Setup virtual environment
    if not setup_virtual_environment():
        print("❌ Failed to set up virtual environment")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Failed to install dependencies")
        sys.exit(1)
    
    # Setup .env file
    setup_env_file()
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file with your PostgreSQL credentials")
    print("2. Ensure PostgreSQL is running on your system")
    print("3. Test the server: python test_server.py")
    print("4. Run the server: python postgres_server.py")
    
    if platform.system() == "Windows":
        print("\nTo activate the virtual environment:")
        print("  venv\\Scripts\\activate")
    else:
        print("\nTo activate the virtual environment:")
        print("  source venv/bin/activate")

if __name__ == "__main__":
    main()
