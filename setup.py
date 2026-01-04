#!/usr/bin/env python3
"""
Setup script for PostgreSQL MCP Server

This script helps set up the development environment and dependencies.
"""

import os
import sys
import subprocess
import platform


def run_command(command: str, description: str) -> bool:
    """Run a shell command and report whether it succeeded.

    Args:
        command: Command to execute.
        description: Human-readable description used for console output.

    Returns:
        True if the command succeeded.

    Raises:
        OSError: If the command cannot be executed.
    """
    print(f"🔄 {description}...")
    try:
        subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as called_process_error:
        print(f"❌ {description} failed: {called_process_error.stderr}")
        return False


def check_python_version() -> bool:
    """Validate that the running Python version meets the project requirement.

    Returns:
        True if the current interpreter is Python 3.10 or newer.
    """
    version = sys.version_info
    if version.major == 3 and version.minor >= 10:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not compatible. Requires Python 3.10+")
        return False


def setup_virtual_environment() -> bool:
    """Create a local virtual environment if it does not already exist.

    Returns:
        True if the virtual environment exists or was created successfully.
    """
    if os.path.exists('venv'):
        print("✅ Virtual environment already exists")
        return True

    return run_command("python -m venv venv", "Creating virtual environment")


def install_dependencies() -> bool:
    """Install dependencies from requirements.txt into the virtual environment.

    Returns:
        True if dependency installation completed successfully.
    """
    pip_command = "venv\\Scripts\\pip" if platform.system() == "Windows" else "venv/bin/pip"

    return run_command(f"{pip_command} install -r requirements.txt", "Installing dependencies")


def setup_env_file() -> bool:
    """Create a .env file from .env.example if needed.

    Returns:
        True if the .env file exists or was created successfully.
    """
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
        except OSError as operating_system_error:
            print(f"❌ Failed to create .env file: {operating_system_error}")
            return False
    else:
        print("❌ .env.example file not found")
        return False


def main() -> None:
    """Set up the local development environment for this repository."""
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
