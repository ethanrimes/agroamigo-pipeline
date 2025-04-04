Sure! Here's your test instructions, converted into clean Markdown with a **"PDF Processor Tests"** heading:

---

## 📄 PDF Processor Tests

### ✅ Running All PDF Processor Tests
```bash
python -m pytest tests/test_pdf_processor.py -v
```

### 🎯 Running a Specific Test Function
```bash
python -m pytest tests/test_pdf_processor.py::TestPDFProcessor::test_single_text_extraction_pdf -v
python -m pytest tests/test_pdf_processor.py::TestPDFProcessor::test_metadata_extraction -v
```

### 📝 Running Text Extraction Tests
```bash
python -m pytest tests/test_text_extraction.py -v
```

### 📦 Running Batch Processing Tests
```bash
python -m pytest tests/test_pdf_processor.py::TestPDFProcessor::test_batch_processing -v
```

### 🐞 Running with Debug Output (show detailed logs)
```bash
python -m pytest tests/test_pdf_processor.py -v --capture=no
```

---

Let me know if you want this added to a README or converted to a shell script!