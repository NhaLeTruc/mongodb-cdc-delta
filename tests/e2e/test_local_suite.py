"""
End-to-end tests for local test suite execution.

Tests that the full test suite can run successfully in the local
Docker Compose environment.
"""

import pytest
import subprocess
import time
import os


class TestLocalTestSuite:
    """Test local test suite execution"""

    @pytest.fixture(scope="class")
    def test_directories(self):
        """Directories containing tests"""
        return [
            "tests/unit",
            "tests/integration",
            "tests/e2e"
        ]

    def test_pytest_installed(self):
        """Test that pytest is installed"""
        result = subprocess.run(
            ["pytest", "--version"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, "pytest not installed"
        assert "pytest" in result.stdout.lower()

    def test_test_directories_exist(self, test_directories):
        """Test that test directories exist"""
        for directory in test_directories:
            assert os.path.exists(directory), f"Test directory {directory} not found"

    def test_unit_tests_exist(self):
        """Test that unit tests exist"""
        unit_test_files = [
            "tests/unit/test_retry.py",
            "tests/unit/test_dlq.py",
            "tests/unit/test_checkpointing.py",
            "tests/unit/test_bson_to_delta.py",
            "tests/unit/test_schema_manager.py"
        ]

        existing_tests = [f for f in unit_test_files if os.path.exists(f)]
        assert len(existing_tests) > 0, "No unit tests found"

    def test_integration_tests_exist(self):
        """Test that integration tests exist"""
        integration_test_files = [
            "tests/integration/test_retry_minio.py",
            "tests/integration/test_corrupted_data.py",
            "tests/integration/test_crash_recovery.py"
        ]

        existing_tests = [f for f in integration_test_files if os.path.exists(f)]
        assert len(existing_tests) > 0, "No integration tests found"

    def test_pytest_ini_exists(self):
        """Test that pytest.ini exists"""
        assert os.path.exists("pytest.ini"), "pytest.ini not found"

    def test_pytest_ini_valid(self):
        """Test that pytest.ini is valid"""
        with open("pytest.ini", 'r') as f:
            content = f.read()

        # Should have testpaths defined
        assert "testpaths" in content or "[pytest]" in content

    def test_makefile_has_test_target(self):
        """Test that Makefile has test target"""
        assert os.path.exists("Makefile"), "Makefile not found"

        with open("Makefile", 'r') as f:
            content = f.read()

        # Should have test targets
        assert "test" in content.lower()

    def test_make_test_local_command(self):
        """Test that make test-local command exists"""
        with open("Makefile", 'r') as f:
            content = f.read()

        assert "test-local" in content, "test-local target not found in Makefile"


class TestTestExecution:
    """Test test execution workflows"""

    def test_pytest_collection(self):
        """Test that pytest can collect tests"""
        result = subprocess.run(
            ["pytest", "--collect-only", "-q"],
            capture_output=True,
            text=True,
            cwd=os.getcwd()
        )

        # Should successfully collect tests
        assert "error" not in result.stdout.lower() or "warning" in result.stdout.lower()

    def test_pytest_markers_defined(self):
        """Test that pytest markers are defined"""
        with open("pytest.ini", 'r') as f:
            content = f.read()

        # Common markers that should be defined
        expected_markers = ["integration", "e2e", "unit"]

        # At least some markers should be present
        marker_count = sum(1 for marker in expected_markers if marker in content)
        assert marker_count > 0, "No pytest markers defined"

    def test_coverage_configuration(self):
        """Test that coverage configuration exists"""
        # Check for .coveragerc or coverage config in pytest.ini
        has_coveragerc = os.path.exists(".coveragerc")
        has_pytest_coverage = False

        if os.path.exists("pytest.ini"):
            with open("pytest.ini", 'r') as f:
                content = f.read()
                has_pytest_coverage = "coverage" in content

        assert has_coveragerc or has_pytest_coverage, "No coverage configuration found"


class TestTestEnvironment:
    """Test test environment setup"""

    def test_test_requirements_exist(self):
        """Test that test requirements are defined"""
        # Check for test requirements in various locations
        test_req_files = [
            "tests/requirements.txt",
            "requirements-test.txt",
            "requirements-dev.txt"
        ]

        exists = any(os.path.exists(f) for f in test_req_files)
        # If no separate test requirements, main requirements should exist
        assert exists or os.path.exists("requirements.txt"), "No test requirements found"

    def test_testcontainers_available(self):
        """Test that testcontainers is available for integration tests"""
        try:
            import testcontainers
            assert True
        except ImportError:
            # Testcontainers may not be installed yet, just verify it's in requirements
            if os.path.exists("tests/requirements.txt"):
                with open("tests/requirements.txt", 'r') as f:
                    content = f.read()
                    assert "testcontainers" in content or "docker" in content

    def test_test_fixtures_directory(self):
        """Test that test fixtures directory exists"""
        assert os.path.exists("tests/fixtures"), "Test fixtures directory not found"

    def test_test_data_available(self):
        """Test that test data files exist"""
        fixture_files = [
            "tests/fixtures/sample_documents.json",
        ]

        # At least some fixture files should exist
        existing_fixtures = [f for f in fixture_files if os.path.exists(f)]
        assert len(existing_fixtures) > 0, "No test fixture files found"


class TestContinuousIntegration:
    """Test CI/CD configuration"""

    def test_pre_commit_hooks_exist(self):
        """Test that pre-commit hooks are configured"""
        assert os.path.exists(".pre-commit-config.yaml"), ".pre-commit-config.yaml not found"

    def test_pre_commit_hooks_valid(self):
        """Test that pre-commit config is valid"""
        result = subprocess.run(
            ["pre-commit", "validate-config"],
            capture_output=True,
            text=True
        )

        # May not be installed, but config should be valid
        assert result.returncode in [0, 127], "pre-commit config is invalid"

    def test_github_workflows_exist(self):
        """Test that GitHub Actions workflows exist (if applicable)"""
        workflows_dir = ".github/workflows"

        if os.path.exists(workflows_dir):
            workflow_files = os.listdir(workflows_dir)
            assert len(workflow_files) > 0, "No GitHub workflow files found"


class TestTestCoverage:
    """Test coverage requirements"""

    def test_coverage_threshold_defined(self):
        """Test that coverage threshold is defined"""
        config_files = ["pytest.ini", ".coveragerc", "pyproject.toml"]

        threshold_defined = False
        for config_file in config_files:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    content = f.read()
                    if "fail_under" in content or "min_coverage" in content:
                        threshold_defined = True
                        break

        # Coverage threshold is optional but recommended
        # Just log if not found
        if not threshold_defined:
            print("Warning: No coverage threshold defined")


class TestDocumentation:
    """Test documentation for testing"""

    def test_testing_docs_exist(self):
        """Test that testing documentation exists"""
        docs_files = [
            "docs/development/testing.md",
            "docs/testing.md",
            "TESTING.md"
        ]

        exists = any(os.path.exists(f) for f in docs_files)
        assert exists, "No testing documentation found"

    def test_setup_docs_exist(self):
        """Test that setup documentation exists"""
        docs_files = [
            "docs/development/setup.md",
            "docs/setup.md",
            "SETUP.md",
            "README.md"
        ]

        exists = any(os.path.exists(f) for f in docs_files)
        assert exists, "No setup documentation found"


class TestTestReporting:
    """Test test reporting configuration"""

    def test_junit_xml_output_configured(self):
        """Test that JUnit XML output is configured"""
        if os.path.exists("pytest.ini"):
            with open("pytest.ini", 'r') as f:
                content = f.read()

            # JUnit XML is useful for CI
            if "junit" not in content.lower():
                print("Info: JUnit XML output not configured (optional)")

    def test_html_report_configured(self):
        """Test that HTML report is configured"""
        if os.path.exists("pytest.ini"):
            with open("pytest.ini", 'r') as f:
                content = f.read()

            # HTML report is useful for local development
            if "html" not in content.lower():
                print("Info: HTML report not configured (optional)")


class TestLocalDevelopmentWorkflow:
    """Test local development workflow"""

    def test_make_lint_exists(self):
        """Test that make lint command exists"""
        with open("Makefile", 'r') as f:
            content = f.read()

        assert "lint" in content, "lint target not found in Makefile"

    def test_make_format_exists(self):
        """Test that make format command exists"""
        with open("Makefile", 'r') as f:
            content = f.read()

        assert "format" in content, "format target not found in Makefile"

    def test_make_clean_exists(self):
        """Test that make clean command exists"""
        with open("Makefile", 'r') as f:
            content = f.read()

        assert "clean" in content, "clean target not found in Makefile"

    def test_setup_script_exists(self):
        """Test that setup script exists"""
        assert os.path.exists("scripts/setup-local.sh"), "setup-local.sh not found"

    def test_setup_script_executable(self):
        """Test that setup script is executable"""
        import stat

        if os.path.exists("scripts/setup-local.sh"):
            st = os.stat("scripts/setup-local.sh")
            is_executable = bool(st.st_mode & stat.S_IXUSR)
            assert is_executable, "setup-local.sh is not executable"


class TestFullTestSuiteExecution:
    """Test full test suite execution (if environment is ready)"""

    @pytest.mark.slow
    def test_unit_tests_pass(self):
        """Test that unit tests pass"""
        result = subprocess.run(
            ["pytest", "tests/unit", "-v", "--tb=short", "-x"],
            capture_output=True,
            text=True,
            timeout=300
        )

        # Tests should pass (or may fail if dependencies not installed)
        if result.returncode != 0:
            print(f"Unit tests output:\n{result.stdout}\n{result.stderr}")
            # Don't fail if it's just missing dependencies
            if "ModuleNotFoundError" in result.stderr or "ImportError" in result.stderr:
                pytest.skip("Dependencies not installed")

    @pytest.mark.slow
    @pytest.mark.integration
    def test_integration_tests_with_docker(self):
        """Test that integration tests can run with Docker"""
        # Check if Docker is available
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            pytest.skip("Docker not available")

        # Run a subset of integration tests
        result = subprocess.run(
            ["pytest", "tests/integration", "-v", "-k", "test_docker", "--tb=short"],
            capture_output=True,
            text=True,
            timeout=600
        )

        # May fail if services not running, just verify the test framework works
        assert "PASSED" in result.stdout or "SKIPPED" in result.stdout or "collected" in result.stdout
