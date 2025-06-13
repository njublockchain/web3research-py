#!/usr/bin/env python3
"""
Automated release tool for web3research-py

This script automates the release process by:
1. Bumping version in pyproject.toml
2. Creating and pushing a Git tag
3. Building and uploading package to PyPI using uv

Authentication is handled via environment variables or .env file:
- PYPI_TOKEN: API token for PyPI (starts with "pypi-")
- TEST_PYPI_TOKEN: API token for TestPyPI (starts with "pypi-")

Usage:
    # Option 1: Using environment variables
    export PYPI_TOKEN=pypi-your-token-here
    python scripts/release.py [patch|minor|major] [--dry-run]
    
    # Option 2: Using .env file
    echo 'PYPI_TOKEN=pypi-your-token-here' >> .env
    python scripts/release.py [patch|minor|major] [--dry-run]
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Tuple, Optional


def load_env_file(env_path: Path) -> None:
    """Load environment variables from .env file"""
    if not env_path.exists():
        return
    
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                # Only set if not already in environment
                if key not in os.environ:
                    os.environ[key] = value


class ReleaseManager:
    def __init__(self, project_root: Path, dry_run: bool = False, repository: Optional[str] = None):
        self.project_root = project_root
        self.pyproject_path = project_root / "pyproject.toml"
        self.init_py_path = project_root / "web3research" / "__init__.py"
        self.docs_conf_path = project_root / "docs" / "source" / "conf.py"
        self.dry_run = dry_run
        self.repository = repository
        
        # Load environment variables from .env file
        env_file = project_root / ".env"
        load_env_file(env_file)
        
    def run_command(self, cmd: list, check: bool = True, capture_output: bool = False) -> subprocess.CompletedProcess:
        """Run a command with optional dry-run mode"""
        print(f"Running: {' '.join(cmd)}")
        
        if self.dry_run:
            print("  (dry-run mode - not actually executing)")
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")
        
        result = subprocess.run(cmd, capture_output=capture_output, text=True, check=check)
        if capture_output and result.stdout:
            print(f"  Output: {result.stdout.strip()}")
        return result
    
    def get_current_version(self) -> str:
        """Get current version from pyproject.toml"""
        content = self.pyproject_path.read_text()
        match = re.search(r'version = "([^"]+)"', content)
        if not match:
            raise ValueError("Could not find version in pyproject.toml")
        return match.group(1)
    
    def bump_version(self, current_version: str, bump_type: str) -> str:
        """Bump version according to semantic versioning"""
        parts = list(map(int, current_version.split('.')))
        
        if len(parts) != 3:
            raise ValueError(f"Invalid version format: {current_version}")
        
        major, minor, patch = parts
        
        if bump_type == "major":
            major += 1
            minor = 0
            patch = 0
        elif bump_type == "minor":
            minor += 1
            patch = 0
        elif bump_type == "patch":
            patch += 1
        else:
            raise ValueError(f"Invalid bump type: {bump_type}")
        
        return f"{major}.{minor}.{patch}"
    
    def update_version_in_file(self, new_version: str) -> None:
        """Update version in pyproject.toml, __init__.py, and docs/conf.py"""
        # Update pyproject.toml
        content = self.pyproject_path.read_text()
        new_content = re.sub(
            r'version = "[^"]+"',
            f'version = "{new_version}"',
            content
        )
        
        if not self.dry_run:
            self.pyproject_path.write_text(new_content)
        
        print(f"Updated version to {new_version} in pyproject.toml")
        
        # Update __init__.py
        if self.init_py_path.exists():
            init_content = self.init_py_path.read_text()
            new_init_content = re.sub(
                r'__version__ = "[^"]+"',
                f'__version__ = "{new_version}"',
                init_content
            )
            
            if not self.dry_run:
                self.init_py_path.write_text(new_init_content)
            
            print(f"Updated __version__ to {new_version} in web3research/__init__.py")
        
        # Check and potentially update docs/source/conf.py
        if self.docs_conf_path.exists():
            docs_content = self.docs_conf_path.read_text()
            
            # Check if version or release is already defined
            has_version = re.search(r'^version\s*=', docs_content, re.MULTILINE)
            has_release = re.search(r'^release\s*=', docs_content, re.MULTILINE)
            
            if has_version or has_release:
                # Update existing version/release variables
                if has_version:
                    new_docs_content = re.sub(
                        r'version\s*=\s*["\'][^"\']*["\']',
                        f'version = "{new_version}"',
                        docs_content
                    )
                    docs_content = new_docs_content
                
                if has_release:
                    new_docs_content = re.sub(
                        r'release\s*=\s*["\'][^"\']*["\']',
                        f'release = "{new_version}"',
                        docs_content
                    )
                    docs_content = new_docs_content
                
                if not self.dry_run:
                    self.docs_conf_path.write_text(docs_content)
                
                print(f"Updated version/release to {new_version} in docs/source/conf.py")
            else:
                # Add version and release after project info if they don't exist
                project_line_match = re.search(r"^(project\s*=\s*['\"].*['\"])\s*$", docs_content, re.MULTILINE)
                if project_line_match:
                    version_lines = f"\nversion = \"{new_version}\"\nrelease = \"{new_version}\""
                    new_docs_content = docs_content.replace(
                        project_line_match.group(0),
                        project_line_match.group(0) + version_lines
                    )
                    
                    if not self.dry_run:
                        self.docs_conf_path.write_text(new_docs_content)
                    
                    print(f"Added version and release {new_version} to docs/source/conf.py")
    
    def check_git_status(self) -> None:
        """Check if git working directory is clean"""
        result = self.run_command(
            ["git", "status", "--porcelain"], 
            capture_output=True
        )
        
        if result.stdout.strip() and not self.dry_run:
            print("Warning: Git working directory is not clean.")
            print("Uncommitted changes:")
            print(result.stdout)
            
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                sys.exit(1)
    
    def check_git_remote(self) -> None:
        """Check if git remote is properly configured"""
        try:
            result = self.run_command(
                ["git", "remote", "get-url", "origin"], 
                capture_output=True
            )
            print(f"Git remote origin: {result.stdout.strip()}")
        except subprocess.CalledProcessError:
            print("Warning: No git remote 'origin' configured")
    
    def commit_version_change(self, new_version: str) -> None:
        """Commit the version change"""
        files_to_add = ["pyproject.toml"]
        
        if self.init_py_path.exists():
            files_to_add.append("web3research/__init__.py")
        
        if self.docs_conf_path.exists():
            # Check if docs/source/conf.py was actually modified
            result = self.run_command(
                ["git", "status", "--porcelain", "docs/source/conf.py"], 
                capture_output=True, 
                check=False
            )
            if result.stdout.strip():
                files_to_add.append("docs/source/conf.py")
        
        for file_path in files_to_add:
            self.run_command(["git", "add", file_path])
        
        self.run_command([
            "git", "commit", "-m", f"Bump version to {new_version}"
        ])
    
    def create_and_push_tag(self, version: str) -> None:
        """Create and push a git tag"""
        tag_name = f"v{version}"
        
        # Create tag
        self.run_command([
            "git", "tag", "-a", tag_name, "-m", f"Release {tag_name}"
        ])
        
        # Push commits and the new tag specifically
        self.run_command(["git", "push"])
        self.run_command(["git", "push", "origin", tag_name])
        
        print(f"Created and pushed tag: {tag_name}")
    
    def build_package(self) -> None:
        """Build the package using uv"""
        print("Building package...")
        
        # Clean previous builds
        build_dir = self.project_root / "dist"
        if build_dir.exists() and not self.dry_run:
            import shutil
            shutil.rmtree(build_dir)
        
        # Build package
        self.run_command(["uv", "build"])
        
        print("Package built successfully")
    
    def upload_to_pypi(self, test: bool = False) -> None:
        """Upload package to PyPI, TestPyPI, or custom repository using uv"""
        if self.repository:
            print(f"Uploading to custom repository: {self.repository}")
            repo_url = self.repository
            token_env_var = "PYPI_TOKEN"  # Use generic token for custom repos
        elif test:
            print("Uploading to TestPyPI...")
            repo_url = "https://test.pypi.org/legacy/"
            token_env_var = "TEST_PYPI_TOKEN"
        else:
            print("Uploading to PyPI...")
            repo_url = "https://upload.pypi.org/legacy/"
            token_env_var = "PYPI_TOKEN"
        
        # Check for authentication token in environment variables
        token = os.getenv(token_env_var)
        if not token:
            print(f"Error: {token_env_var} environment variable not set.")
            print(f"Please set the {token_env_var} environment variable with your PyPI API token.")
            print(f"You can either:")
            print(f"  1. Export it: export {token_env_var}=pypi-...")
            print(f"  2. Add it to .env file: echo '{token_env_var}=pypi-...' >> .env")
            sys.exit(1)
        
        # Upload using uv publish
        upload_cmd = ["uv", "publish"]
        upload_cmd.extend(["--repository-url", repo_url])
        upload_cmd.extend(["--username", "__token__"])
        upload_cmd.extend(["--password", token])
        
        self.run_command(upload_cmd)
        print("Package uploaded successfully")
    
    def release(self, bump_type: str, test_pypi: bool = False) -> None:
        """Main release workflow"""
        print(f"Starting release process with bump type: {bump_type}")
        print(f"Dry run mode: {self.dry_run}")
        
        # Check git status
        self.check_git_status()
        self.check_git_remote()
        
        # Get current version and bump it
        current_version = self.get_current_version()
        new_version = self.bump_version(current_version, bump_type)
        
        print(f"Current version: {current_version}")
        print(f"New version: {new_version}")
        
        if not self.dry_run:
            response = input(f"Proceed with release {new_version}? (y/N): ")
            if response.lower() != 'y':
                print("Release cancelled")
                return
        
        # Update version
        self.update_version_in_file(new_version)
        
        # Commit changes
        self.commit_version_change(new_version)
        
        # Create and push tag
        self.create_and_push_tag(new_version)
        
        # Build package
        self.build_package()
        
        # Upload to PyPI
        self.upload_to_pypi(test=test_pypi)
        
        print(f"Release {new_version} completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Automated release tool for web3research-py")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Release command
    release_parser = subparsers.add_parser('release', help='Perform a full release')
    release_parser.add_argument(
        "bump_type", 
        choices=["patch", "minor", "major"],
        help="Type of version bump to perform"
    )
    release_parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Show what would be done without actually doing it"
    )
    release_parser.add_argument(
        "--repository", 
        type=str,
        help="Custom PyPI repository URL (e.g., https://your-repo.com/simple/)"
    )
    release_parser.add_argument(
        "--test-pypi", 
        action="store_true",
        help="Upload to TestPyPI instead of PyPI"
    )
    
    # Simplified commands (for backward compatibility and direct usage)
    for bump_type in ["patch", "minor", "major"]:
        cmd_parser = subparsers.add_parser(bump_type, help=f'Release {bump_type} version')
        cmd_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
        cmd_parser.add_argument("--repository", type=str, help="Custom PyPI repository URL")
        cmd_parser.add_argument("--test-pypi", action="store_true", help="Upload to TestPyPI")
    
    # Version command
    version_parser = subparsers.add_parser('version', help='Show current version')
    
    # Build command
    build_parser = subparsers.add_parser('build', help='Build package only')
    build_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    
    # Upload command
    upload_parser = subparsers.add_parser('upload', help='Upload package to PyPI')
    upload_parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    upload_parser.add_argument("--repository", type=str, help="Custom PyPI repository URL")
    upload_parser.add_argument("--test-pypi", action="store_true", help="Upload to TestPyPI")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Get project root (parent of scripts directory)
    project_root = Path(__file__).parent.parent
    
    # Create release manager
    release_manager = ReleaseManager(
        project_root, 
        dry_run=getattr(args, 'dry_run', False),
        repository=getattr(args, 'repository', None)
    )
    
    try:
        if args.command == 'version':
            version = release_manager.get_current_version()
            print(f"Current version: {version}")
        
        elif args.command == 'release':
            release_manager.release(args.bump_type, test_pypi=args.test_pypi)
        
        elif args.command in ['patch', 'minor', 'major']:
            release_manager.release(args.command, test_pypi=getattr(args, 'test_pypi', False))
        
        elif args.command == 'build':
            release_manager.build_package()
        
        elif args.command == 'upload':
            release_manager.upload_to_pypi(test=getattr(args, 'test_pypi', False))
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
