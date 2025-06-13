#!/bin/bash
# Simple shell wrapper for the release tool

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Function to show usage
show_usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  patch       - Release a patch version (x.y.Z)"
    echo "  minor       - Release a minor version (x.Y.0)"
    echo "  major       - Release a major version (X.0.0)"
    echo "  version     - Show current version"
    echo "  build       - Build package only"
    echo "  upload      - Upload built package to PyPI"
    echo ""
    echo "Options:"
    echo "  --dry-run     - Show what would be done without doing it"
    echo "  --test-pypi   - Use TestPyPI instead of PyPI"
    echo "  --repository  - Custom PyPI repository URL"
    echo ""
    echo "Examples:"
    echo "  $0 patch                              # Release patch version"
    echo "  $0 minor --dry-run                    # Dry run minor release"
    echo "  $0 upload --test-pypi                 # Upload to TestPyPI"
    echo "  $0 upload --repository https://my.pypi.org/simple/"
}

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "Error: uv is not installed. Please install it first:"
    echo "curl -LsSf https://astral.sh/uv/install.sh | sh"
    exit 1
fi

# Parse arguments and delegate to release.py
case "$1" in
    patch|minor|major)
        command="$1"
        shift
        python3 "$SCRIPT_DIR/release.py" "$command" "$@"
        ;;
    version)
        python3 -c "
import sys
sys.path.insert(0, '$SCRIPT_DIR')
from release import ReleaseManager
from pathlib import Path
rm = ReleaseManager(Path('$PROJECT_ROOT'))
print(f'Current version: {rm.get_current_version()}')
"
        ;;
    build)
        shift
        python3 -c "
import sys
sys.path.insert(0, '$SCRIPT_DIR')
from release import ReleaseManager
from pathlib import Path
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--dry-run', action='store_true')
args = parser.parse_args()
rm = ReleaseManager(Path('$PROJECT_ROOT'), dry_run=args.dry_run)
rm.build_package()
" "$@"
        ;;
    upload)
        shift
        python3 -c "
import sys
sys.path.insert(0, '$SCRIPT_DIR')
from release import ReleaseManager
from pathlib import Path
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--dry-run', action='store_true')
parser.add_argument('--test-pypi', action='store_true')
parser.add_argument('--repository', type=str)
args = parser.parse_args()
rm = ReleaseManager(Path('$PROJECT_ROOT'), dry_run=args.dry_run, repository=args.repository)
rm.upload_to_pypi(test=args.test_pypi)
" "$@"
        ;;
    -h|--help|help)
        show_usage
        ;;
    "")
        show_usage
        exit 1
        ;;
    *)
        echo "Error: Unknown command '$1'"
        echo ""
        show_usage
        exit 1
        ;;
esac
