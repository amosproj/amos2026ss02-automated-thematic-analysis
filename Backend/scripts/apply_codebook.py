import argparse
import json
import sys
from pathlib import Path

# Add Backend root to path so we can import 'app' modules
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

from app.llm.pipelines import apply_codebook_to_interview
from app.schemas.llm import InterviewAnalysisResult

def main():
    parser = argparse.ArgumentParser(description="Apply a codebook to an interview transcript using an LLM.")
    parser.add_argument("--interview", type=str, required=True, help="Path to the interview transcript text file.")
    parser.add_argument("--codebook", type=str, required=True, help="Path to the codebook JSON file.")
    args = parser.parse_args()

    # Read interview
    with open(args.interview, "r", encoding="utf-8") as f:
        transcript = f.read()

    # Read codebook
    with open(args.codebook, "r", encoding="utf-8") as f:
        codebook_data = json.load(f)

    # Format codebook context
    # Expects list of dicts with 'label' (or 'name') and 'description'
    codebook_lines = []
    for item in codebook_data:
        name = item.get("label") or item.get("name") or "Unnamed Theme"
        desc = item.get("description") or "No description provided."
        codebook_lines.append(f"Theme: {name}\nDefinition: {desc}\n")
    
    codebook_context = "\n".join(codebook_lines)

    print(f"Applying codebook ({len(codebook_data)} themes) to interview ({len(transcript)} chars)...", flush=True)
    
    try:
        result: InterviewAnalysisResult = apply_codebook_to_interview(transcript, codebook_context)
    except Exception as e:
        print(f"Error during LLM invocation: {e}", flush=True)
        sys.exit(1)

    # Print results nicely
    print("\n" + "="*40)
    print("RESULTS")
    print("="*40)
    
    if result.summary:
        print(f"\nSummary:\n{result.summary}\n")

    print(f"{'THEME':<30} | {'PRESENT':<7} | {'CONFIDENCE':<10} | {'QUOTE'}")
    print("-" * 80)
    for t in result.themes:
        present_str = "YES" if t.present else "NO"
        quote_preview = (t.quote[:50] + "...") if t.quote and t.present else "N/A"
        print(f"{t.theme_label[:28]:<30} | {present_str:<7} | {t.confidence:<10.2f} | {quote_preview}")

    if result.researcher_notes:
        print(f"\nNotes:\n{result.researcher_notes}\n")

if __name__ == "__main__":
    main()
