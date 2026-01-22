#!/bin/bash
# Cleanup script for py-analysis-worker

echo "🧹 Cleaning up py-analysis-worker..."

# Remove Python cache files
echo "Removing Python cache files..."
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true
find . -name "*.pyo" -delete 2>/dev/null || true

# Remove temporary OCR files
echo "Removing temporary OCR files..."
find . -name "*_enhanced.*" -delete 2>/dev/null || true
find . -name "*_page_*.png" -delete 2>/dev/null || true
find . -name "*_page_*.jpg" -delete 2>/dev/null || true

# Remove OS generated files
echo "Removing OS files..."
find . -name ".DS_Store" -delete 2>/dev/null || true
find . -name "Thumbs.db" -delete 2>/dev/null || true

# Remove log files
echo "Removing log files..."
find . -name "*.log" -delete 2>/dev/null || true

# Remove temporary directories
echo "Removing temporary directories..."
rm -rf temp/ tmp/ 2>/dev/null || true

# Remove backup files
echo "Removing backup files..."
find . -name "*.bak" -delete 2>/dev/null || true
find . -name "*.backup" -delete 2>/dev/null || true

echo "✅ Cleanup completed!"
echo ""
echo "Files that remain ignored by .gitignore:"
echo "- .env (environment variables)"
echo "- venv/ (virtual environment)"
echo "- Any new __pycache__/ directories"
echo "- Any new temporary OCR files"