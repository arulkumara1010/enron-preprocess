# ==============================================================================
# preprocess.py
#
# This script performs end-to-end preprocessing of the Enron email dataset.
# It does the following:
# 1. Parses raw email files from the 'maildir' directory structure.
# 2. Cleans the email body to remove replies, signatures, and headers.
# 3. Uses the Presidio library to find and redact PII (anonymization).
# 4. Saves the final, clean, and anonymized dataset to a Parquet file.
#
# To run: python3 preprocess.py
# ==============================================================================

import os
import pandas as pd
import re
from email import message_from_string
from tqdm import tqdm
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig


# --- Text Cleaning Function ---
def clean_text(text):
    """Removes boilerplate text from an email body."""
    if not isinstance(text, str):
        return ""

    # Remove forwarded message headers and basic headers
    text = re.sub(
        r"-----Original Message-----.*", "", text, flags=re.DOTALL | re.IGNORECASE
    )
    text = re.sub(r"From:.*", "", text)
    text = re.sub(r"To:.*", "", text)
    text = re.sub(r"Sent:.*", "", text)
    text = re.sub(r"Subject:.*", "", text)

    # Remove quoted reply text (lines starting with '>')
    text = re.sub(r">.*", "", text)

    # Remove common email signatures and boilerplate
    text = re.sub(r"Sincerely.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"Best regards.*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(
        r"Confidentiality Notice.*", "", text, flags=re.IGNORECASE | re.DOTALL
    )

    # Remove extra whitespace and blank lines
    text = "\n".join([line for line in text.split("\n") if line.strip() != ""])
    return text.strip()


# --- PII Anonymization Function ---
def anonymize_text(text, analyzer, anonymizer):
    """Analyzes and anonymizes PII in a given text."""
    if not text:
        return ""
    try:
        # Analyze the text to find PII entities
        analyzer_results = analyzer.analyze(text=text, language="en")

        # Anonymize the text, replacing entities with a tag (e.g., <PERSON>)
        anonymized_result = anonymizer.anonymize(
            text=text,
            analyzer_results=analyzer_results,
            operators={
                "DEFAULT": OperatorConfig("replace", {"new_value": "<REDACTED>"})
            },
        )
        return anonymized_result.text
    except Exception as e:
        # If Presidio fails on a specific text, return the original text
        print(f"Anonymization failed for a text snippet: {e}")
        return text


def main():
    """Main function to run the entire preprocessing pipeline."""

    # --- 1. Load and Parse Emails ---
    print("Step 1: Loading and parsing raw email files...")
    data = []
    enron_path = "./maildir"

    all_files = [
        os.path.join(root, file_name)
        for root, dirs, files in os.walk(enron_path)
        for file_name in files
    ]

    for file_path in tqdm(all_files, desc="Parsing Emails"):
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                # Read the whole file as a string for the email library
                file_content = f.read()
                message = message_from_string(file_content)

                body = ""
                if message.is_multipart():
                    for part in message.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(
                                "latin-1", errors="ignore"
                            )
                            break
                else:
                    body = message.get_payload(decode=True).decode(
                        "latin-1", errors="ignore"
                    )

                data.append({"sender": message.get("From"), "body": body})
        except Exception as e:
            # Skip corrupted or non-standard files
            pass

    df = pd.DataFrame(data)
    print(f"Successfully loaded and parsed {len(df)} emails.")

    # --- 2. Clean Email Bodies ---
    print("\nStep 2: Cleaning email body text...")
    df["cleaned_body"] = [
        clean_text(body) for body in tqdm(df["body"], desc="Cleaning Text")
    ]

    # --- 3. Anonymize PII ---
    print("\nStep 3: Anonymizing PII (this may take a while)...")
    # Initialize Presidio engines once for efficiency
    analyzer = AnalyzerEngine()
    anonymizer = AnonymizerEngine()

    df["anonymized_body"] = [
        anonymize_text(text, analyzer, anonymizer)
        for text in tqdm(df["cleaned_body"], desc="Redacting PII")
    ]

    # --- 4. Save Final Dataset ---
    print("\nStep 4: Saving the final, processed dataset...")
    final_df = df[["sender", "anonymized_body"]].copy()
    final_df.rename(columns={"anonymized_body": "text"}, inplace=True)

    # Remove rows where the text is empty after cleaning
    final_df = final_df[final_df["text"].str.strip() != ""]

    output_filename = "enron_anonymized.parquet"
    final_df.to_parquet(output_filename, index=False)

    print(f"\nPreprocessing complete!")
    print(f"Anonymized dataset with {len(final_df)} emails saved to: {output_filename}")


if __name__ == "__main__":
    main()
