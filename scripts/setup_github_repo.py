#!/usr/bin/env python3
"""
GitHub Repository Setup Script

This script helps configure your GitHub repository with proper description,
topics, and other metadata using the GitHub API.

Requirements:
- GitHub Personal Access Token (with repo permissions)
- requests library: pip install requests

Usage:
    python scripts/setup_github_repo.py --token YOUR_GITHUB_TOKEN
    
Or set environment variable:
    export GITHUB_TOKEN=your_token_here
    python scripts/setup_github_repo.py
"""

import os
import sys
import json
import argparse
import requests
from typing import List, Dict, Any


class GitHubRepoConfig:
    def __init__(self, token: str, owner: str = "mukul975", repo: str = "postgres-mcp-server"):
        self.token = token
        self.owner = owner
        self.repo = repo
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json"
        }
    
    def update_repository_details(self) -> bool:
        """Update repository description and website."""
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}"
        
        data = {
            "name": self.repo,
            "description": "🔄 PostgreSQL MCP Server – AI-Powered PostgreSQL Management & Monitoring. A powerful, AI-integrated PostgreSQL Model Context Protocol (MCP) server for automated database operations, monitoring, security, diagnostics, and optimization. Seamlessly manage PostgreSQL with 237+ tools designed for AI assistants like Claude and ChatGPT.",
            "homepage": f"https://github.com/{self.owner}/{self.repo}",
            "has_issues": True,
            "has_projects": True,
            "has_wiki": True,
            "has_discussions": True,
            "allow_squash_merge": True,
            "allow_merge_commit": True,
            "allow_rebase_merge": True,
            "delete_branch_on_merge": True
        }
        
        try:
            response = requests.patch(url, headers=self.headers, data=json.dumps(data))
            if response.status_code == 200:
                print("✅ Repository details updated successfully")
                return True
            else:
                print(f"❌ Failed to update repository details: {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error updating repository details: {e}")
            return False
    
    def update_topics(self) -> bool:
        """Update repository topics/tags."""
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/topics"
        
        # GitHub allows max 20 topics
        topics = [
            "postgresql",
            "mcp-server", 
            "ai-database-tools",
            "claude-integration",
            "chatgpt-tools",
            "database-monitoring",
            "database-management",
            "sql-automation",
            "database-diagnostics",
            "database-optimization",
            "postgres-tools",
            "ai-integration",
            "database-security",
            "query-optimizer",
            "database-health-check",
            "python-postgresql",
            "devops-automation",
            "database-admin",
            "database-performance",
            "postgres-monitoring"
        ]
        
        data = {"names": topics}
        
        # Need to use different Accept header for topics API
        headers = self.headers.copy()
        headers["Accept"] = "application/vnd.github.mercy-preview+json"
        
        try:
            response = requests.put(url, headers=headers, data=json.dumps(data))
            if response.status_code == 200:
                print(f"✅ Topics updated successfully: {len(topics)} topics added")
                for topic in topics:
                    print(f"   📌 {topic}")
                return True
            else:
                print(f"❌ Failed to update topics: {response.status_code}")
                print(f"Response: {response.text}")
                return False
        except Exception as e:
            print(f"❌ Error updating topics: {e}")
            return False
    
    def create_labels(self) -> bool:
        """Create custom labels for the repository."""
        url = f"{self.base_url}/repos/{self.owner}/{self.repo}/labels"
        
        labels = [
            {"name": "ai-integration", "color": "0052cc", "description": "AI assistant integration related"},
            {"name": "postgresql", "color": "336791", "description": "PostgreSQL specific issues"},
            {"name": "mcp", "color": "28a745", "description": "Model Context Protocol related"},
            {"name": "performance", "color": "ff6b6b", "description": "Performance improvements"},
            {"name": "security", "color": "d73a49", "description": "Security-related issues"},
            {"name": "database-tools", "color": "0e8a16", "description": "Database tooling and utilities"},
            {"name": "monitoring", "color": "1d76db", "description": "Database monitoring features"},
            {"name": "diagnostics", "color": "f9ca24", "description": "Database diagnostics and analysis"}
        ]
        
        created_count = 0
        for label in labels:
            try:
                response = requests.post(url, headers=self.headers, data=json.dumps(label))
                if response.status_code == 201:
                    print(f"✅ Created label: {label['name']}")
                    created_count += 1
                elif response.status_code == 422:
                    print(f"⚠️  Label already exists: {label['name']}")
                else:
                    print(f"❌ Failed to create label {label['name']}: {response.status_code}")
            except Exception as e:
                print(f"❌ Error creating label {label['name']}: {e}")
        
        print(f"📌 Created {created_count} new labels")
        return True
    
    def enable_features(self) -> bool:
        """Enable repository features like discussions."""
        # Note: Some features like discussions require special API calls
        # For now, this is a placeholder for future enhancements
        print("ℹ️  Some features need to be enabled manually in GitHub settings:")
        print("   - Discussions: Go to Settings > General > Features")
        print("   - Security & Analysis: Go to Settings > Security & analysis")
        return True
    
    def run_setup(self) -> bool:
        """Run the complete repository setup."""
        print("🚀 Setting up GitHub repository configuration")
        print("=" * 60)
        print(f"Repository: {self.owner}/{self.repo}")
        print()
        
        success = True
        
        # Update repository details
        print("1️⃣ Updating repository details...")
        if not self.update_repository_details():
            success = False
        print()
        
        # Update topics
        print("2️⃣ Updating repository topics...")
        if not self.update_topics():
            success = False
        print()
        
        # Create labels
        print("3️⃣ Creating custom labels...")
        if not self.create_labels():
            success = False
        print()
        
        # Enable features
        print("4️⃣ Repository feature recommendations...")
        self.enable_features()
        print()
        
        if success:
            print("🎉 Repository setup completed successfully!")
            print(f"📝 View your repository: https://github.com/{self.owner}/{self.repo}")
        else:
            print("⚠️  Repository setup completed with some warnings")
        
        return success


def main():
    parser = argparse.ArgumentParser(description="Setup GitHub repository configuration")
    parser.add_argument(
        "--token", 
        help="GitHub Personal Access Token (or set GITHUB_TOKEN env var)"
    )
    parser.add_argument(
        "--owner", 
        default="mukul975", 
        help="Repository owner (default: mukul975)"
    )
    parser.add_argument(
        "--repo", 
        default="postgres-mcp-server", 
        help="Repository name (default: postgres-mcp-server)"
    )
    
    args = parser.parse_args()
    
    # Get token from args or environment
    token = args.token or os.getenv("GITHUB_TOKEN")
    if not token:
        print("❌ GitHub token required!")
        print("Either use --token argument or set GITHUB_TOKEN environment variable")
        print("Get token from: https://github.com/settings/tokens")
        print("Required permissions: repo (full control of private repositories)")
        sys.exit(1)
    
    # Setup repository
    config = GitHubRepoConfig(token, args.owner, args.repo)
    success = config.run_setup()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
