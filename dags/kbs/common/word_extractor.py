import subprocess
import os
from pathlib import Path
from kbs.common.pdf_processor import extract_pdf_data

def word_to_pdf(doc_filename: str, output_dir="tmp"):
    """Converts an Doc/Docx file to PDF."""
    subprocess.run(
        [
            "libreoffice",
            "--headless",
            "--convert-to",
            "pdf",
            os.path.join(doc_filename),
            "--outdir",
            output_dir,
        ],
        check=True,
    )
    pre, _ = os.path.splitext(doc_filename)
    # Cleanup the temporary docx file
    return os.path.join(pre + '.pdf')

def extract_word_data(minio_client, doc_file, minio_path):
    """
    Extracts data and images from an Word file (doc or docx).

    Args:
      doc_file: Path to the Word file.
    """
    if not (doc_file.endswith(".doc") or doc_file.endswith(".docx")):
        raise ValueError("path must be an excel file")

    doc = word_to_pdf(doc_file)
    # os.remove(doc)
    return extract_pdf_data(minio_client, doc, minio_path)