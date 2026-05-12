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
    parser = argparse.ArgumentParser(description="Apply a codebook to interview transcripts using an LLM.")
    parser.add_argument("--interviews", nargs="+", type=str, required=True, help="One or more paths to interview transcript text files.")
    parser.add_argument("--codebook", type=str, required=True, help="Path to the codebook JSON file.")
    args = parser.parse_args()

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

    from collections import defaultdict
    theme_to_docs = defaultdict(list)
    all_theme_labels = set()

    for interview_path in args.interviews:
        # Read interview
        with open(interview_path, "r", encoding="utf-8") as f:
            transcript = f.read()

        file_name = Path(interview_path).name
        print(f"\nApplying codebook ({len(codebook_data)} themes) to '{file_name}' ({len(transcript)} chars)...", flush=True)
        
        try:
            result: InterviewAnalysisResult = apply_codebook_to_interview(transcript, codebook_context)
        except Exception as e:
            print(f"Error during LLM invocation for {file_name}: {e}", flush=True)
            continue

        # Print results nicely
        print("-" * 80)
        print(f"{'THEME':<30} | {'PRESENT':<7} | {'CONFIDENCE':<10} | {'QUOTE'}")
        print("-" * 80)
        for t in result.themes:
            all_theme_labels.add(t.theme_label)
            if t.present:
                theme_to_docs[t.theme_label].append(file_name)
            
            present_str = "YES" if t.present else "NO"
            quote_preview = (t.quote[:50] + "...") if t.quote and t.present else "N/A"
            print(f"{t.theme_label[:28]:<30} | {present_str:<7} | {t.confidence:<10.2f} | {quote_preview}")

        if result.summary:
            print(f"\nSummary: {result.summary}\n")

    # Aggregate frequencies
    print("\n" + "="*50)
    print("EXPERIMENT RESULTS: THEME FREQUENCIES")
    print("="*50)
    
    for label in sorted(all_theme_labels):
        docs = theme_to_docs.get(label, [])
        count = len(docs)
        print(f"Theme: {label:<25} | Present in {count}/{len(args.interviews)} interviews")
        if count > 0:
            print(f"       -> Found in: {', '.join(docs)}")

if __name__ == "__main__":
    main()
