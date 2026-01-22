# py-analysis-worker

Python worker service for processing slides (OCR + embeddings) in the OratorAI system.

## 🏗️ Project Structure

```
py-analysis-worker/
├── src/                           # Source code
│   ├── processors/                # Processing modules
│   │   ├── __init__.py
│   │   ├── ocr_processor.py      # OCR text extraction
│   │   └── slide_processor.py    # Main slide processing logic
│   ├── clients/                   # External service clients
│   │   ├── __init__.py
│   │   ├── aws_client.py         # AWS SQS & S3 client
│   │   └── webhook_client.py     # Webhook communication
│   ├── handlers/                  # Message handlers
│   │   ├── __init__.py
│   │   └── message_handler.py    # SQS message processing
│   └── __init__.py
├── config/                        # Configuration
│   ├── __init__.py
│   ├── config.py                 # Environment variables & settings
│   └── env.example               # Environment template
├── scripts/                       # Entry point scripts
│   ├── main.py                   # Main polling loop
│   └── run_worker.py             # Alternative entry point
├── docs/                          # Documentation
│   └── README.md                 # Detailed documentation
├── worker.py                      # Main entry point (run this!)
├── requirements.txt               # Python dependencies
└── __init__.py                   # Package initialization
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
# Install system dependencies (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y tesseract-ocr tesseract-ocr-vie poppler-utils

# Install Python dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy environment template
cp config/env.example .env

# Edit environment variables
nano .env
```

### 3. Run the Worker

```bash
# Recommended: Use main entry point
python3 worker.py

# Alternative methods:
python3 scripts/main.py
python3 scripts/run_worker.py
```

## 📋 Features

- **Modular Architecture**: Clean separation of concerns
- **Asynchronous Processing**: AWS SQS integration
- **Multi-format Support**: PDF, images (PNG, JPG, etc.)
- **OCR Text Extraction**: Vietnamese/English support
- **PDF Multi-page**: Process each page separately
- **Image Enhancement**: OpenCV preprocessing for better OCR
- **Webhook Notifications**: Real-time result reporting
- **Error Handling**: Robust error handling and logging

## 🔧 Module Overview

### 📁 src/processors/
- **`ocr_processor.py`**: Handles OCR text extraction using pytesseract and easyocr
- **`slide_processor.py`**: Main processing logic, file downloads, and orchestration

### 📁 src/clients/
- **`aws_client.py`**: AWS SQS and S3 operations wrapper
- **`webhook_client.py`**: HTTP webhook communication with Node API

### 📁 src/handlers/
- **`message_handler.py`**: SQS message processing and workflow orchestration

### 📁 config/
- **`config.py`**: Environment variables, validation, and library availability checks

### 📁 scripts/
- **`main.py`**: Main polling loop and application entry point
- **`run_worker.py`**: Alternative entry point script

## 🔍 Processing Flow

1. **Poll SQS** → Receive slide processing jobs
2. **Download** → Get slide file from S3 (authenticated)
3. **Process** → OCR extraction (PDF multi-page support)
4. **Enhance** → Image preprocessing for better OCR
5. **Webhook** → Send results to Node API
6. **Cleanup** → Remove temporary files and SQS message

## 🛠️ Development

### Adding New Features

The modular structure makes it easy to extend:

- **New OCR Engine**: Add to `src/processors/ocr_processor.py`
- **New File Format**: Extend `src/processors/slide_processor.py`
- **New Client**: Add to `src/clients/`
- **New Handler**: Add to `src/handlers/`

### Testing

```bash
# Test imports
python3 -c "
import sys
sys.path.insert(0, '.')
from src.processors import get_ocr_processor
from src.clients import get_aws_client
from src.handlers import get_message_handler
print('✅ All modules imported successfully')
"

# Test configuration
python3 -c "
import sys
sys.path.insert(0, '.')
from config.config import validate_config
validate_config()
print('✅ Configuration is valid')
"
```

## 📝 Environment Variables

Required in `.env` file:

```env
# AWS Configuration
AWS_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SQS_SLIDES_QUEUE_URL=https://sqs.region.amazonaws.com/account/queue

# Webhook Configuration
WEBHOOK_URL=http://localhost:8080/api/v1/webhooks/slides-complete
WEBHOOK_SECRET=your_webhook_secret

# OCR Configuration (Optional)
OCR_LANGUAGE=vie+eng
```

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure you're running from the project root directory
2. **Module Not Found**: Check that all `__init__.py` files are present
3. **OCR Issues**: Install tesseract and language packs
4. **PDF Processing**: Install poppler-utils
5. **AWS Access**: Verify credentials and permissions

### Logs

Adjust logging level in `config/config.py`:

```python
logging.basicConfig(level=logging.DEBUG)  # For verbose output
```

## 📈 TODO

- [ ] Add unit tests for each module
- [ ] Implement actual embedding generation
- [ ] Add Docker support
- [ ] Add metrics and monitoring
- [ ] Support for more file formats
- [ ] Batch processing capabilities