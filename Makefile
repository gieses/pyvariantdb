.PHONY: help build install clean format lint test pixi-install pixi-update publish publish-test all

# Default target
help:
	@echo "Available targets:"
	@echo "  help          - Show this help message"
	@echo "  build         - Build the package distribution"
	@echo "  clean         - Remove build artifacts and cache files"
	@echo "  ruff          - Run Ruff on files."
	@echo "  test          - Run tests with pytest"
	@echo "  pixi  - Install dependencies with pixi"
	@echo "  publish       - Build and publish to PyPI"
	@echo "  dbsnp       - Download dbSNP and build database"

# Build the package
build: clean
	@echo "Building package..."
	python -m build


# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

# Format code with ruff
ruff:
	@echo "Formatting code with ruff..."
	ruff format .
	ruff check --fix .

# Update pixi dependencies
pixi:
	@echo "Updating pixi dependencies..."
	pixi update
	pixi install

# Publish to PyPI
publish: build
	@echo "Publishing to PyPI..."
	@echo "⚠️  This will upload to the real PyPI!"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		twine upload dist/*; \
	else \
		echo "Upload cancelled."; \
	fi

# Download dbSNP
dbsnp:
	pyvariantdb-download && pyvariantdb-make-dbsnp
