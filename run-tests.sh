#!/bin/bash

# Unified test runner for SparkApp
# Runs both Scala (SBT) tests and Python tests
# Exits with error code if any tests fail

set -e  # Exit on first error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the project root directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "=========================================="
echo "Running SparkApp Test Suite"
echo "=========================================="
echo ""

# Track test results
SCALA_TESTS_PASSED=false
PYTHON_TESTS_PASSED=false
EXIT_CODE=0

# ==========================================
# 1. Run Scala Tests (SBT)
# ==========================================
echo "${YELLOW}[1/2] Running Scala tests with SBT...${NC}"
echo ""

if build/sbt test; then
    echo ""
    echo -e "${GREEN}✓ Scala tests PASSED${NC}"
    SCALA_TESTS_PASSED=true
else
    echo ""
    echo -e "${RED}✗ Scala tests FAILED${NC}"
    EXIT_CODE=1
fi

echo ""
echo "=========================================="
echo ""

# ==========================================
# 2. Run Python Tests
# ==========================================
echo "${YELLOW}[2/2] Running Python integration tests...${NC}"
echo ""

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo -e "${YELLOW}Warning: pytest not found. Trying to run tests directly...${NC}"

    # Run tests directly with Python
    if python3 tests/test_spark_shell.py; then
        echo ""
        echo -e "${GREEN}✓ Python tests PASSED${NC}"
        PYTHON_TESTS_PASSED=true
    else
        echo ""
        echo -e "${RED}✗ Python tests FAILED${NC}"
        EXIT_CODE=1
    fi
else
    # Run with pytest
    if python3 -m pytest tests/test_spark_shell.py -v; then
        echo ""
        echo -e "${GREEN}✓ Python tests PASSED${NC}"
        PYTHON_TESTS_PASSED=true
    else
        echo ""
        echo -e "${RED}✗ Python tests FAILED${NC}"
        EXIT_CODE=1
    fi
fi

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo ""

# Print summary
if [ "$SCALA_TESTS_PASSED" = true ]; then
    echo -e "Scala Tests:  ${GREEN}PASSED ✓${NC}"
else
    echo -e "Scala Tests:  ${RED}FAILED ✗${NC}"
fi

if [ "$PYTHON_TESTS_PASSED" = true ]; then
    echo -e "Python Tests: ${GREEN}PASSED ✓${NC}"
else
    echo -e "Python Tests: ${RED}FAILED ✗${NC}"
fi

echo ""
echo "=========================================="
echo ""

# Final result
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All tests passed! ✓${NC}"
else
    echo -e "${RED}Some tests failed! ✗${NC}"
fi

echo ""

exit $EXIT_CODE
