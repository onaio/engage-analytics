#!/usr/bin/env python3
"""
Extract question text from questionnaire JSON files to create human-readable labels
"""
import json
import os
import glob
import re
from pathlib import Path

def clean_text(text):
    """Clean up question text for use as a label"""
    if not text:
        return ""
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove variable placeholders like [patient-name]
    text = re.sub(r'\[[\w-]+\]', '', text)
    
    # Remove extra whitespace
    text = ' '.join(text.split())
    
    # Truncate if too long (max 100 chars)
    if len(text) > 100:
        text = text[:97] + "..."
    
    return text.strip()

def extract_items_recursive(items, linkid_map, parent_context=""):
    """Recursively extract linkIds and text from questionnaire items"""
    if not items:
        return
    
    for item in items:
        if not isinstance(item, dict):
            continue
            
        linkid = item.get('linkId', '')
        text = item.get('text', '')
        item_type = item.get('type', '')
        
        # Clean the text
        clean = clean_text(text)
        
        # If it's a meaningful question/item, add to map
        if linkid and clean:
            # Add context for nested items
            if parent_context and item_type not in ['group', 'display']:
                full_text = f"{parent_context} - {clean}" if parent_context != clean else clean
            else:
                full_text = clean
                
            linkid_map[linkid] = full_text
        elif linkid and not clean:
            # For items without text, use the type or parent context
            if item_type == 'group':
                linkid_map[linkid] = parent_context or "Group"
            elif item_type == 'display':
                linkid_map[linkid] = "Display Text"
            elif parent_context:
                linkid_map[linkid] = parent_context
        
        # Process answer options if they have linkIds
        answer_option = item.get('answerOption', [])
        for answer in answer_option:
            if isinstance(answer, dict):
                value_coding = answer.get('valueCoding', {})
                if isinstance(value_coding, dict):
                    code = value_coding.get('code', '')
                    display = value_coding.get('display', '')
                    if code and display:
                        # Some answers use their code as linkId
                        linkid_map[code] = display
        
        # Recursively process nested items
        nested_items = item.get('item', [])
        if nested_items:
            # Use current item's text as context for nested items
            new_context = clean if clean and item_type == 'group' else parent_context
            extract_items_recursive(nested_items, linkid_map, new_context)

def process_questionnaire_file(filepath):
    """Process a single questionnaire JSON file"""
    linkid_map = {}
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        # Get questionnaire title
        title = data.get('title', Path(filepath).stem)
        
        # Process all items
        items = data.get('item', [])
        extract_items_recursive(items, linkid_map, title)
        
        return linkid_map
    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return {}

def main():
    """Process all questionnaire files and create linkId mapping"""
    
    # Find all questionnaire JSON files
    questionnaire_dir = "/Users/mberg/github/engage-analytics/questionnaire"
    json_files = glob.glob(f"{questionnaire_dir}/**/*.json", recursive=True)
    
    # Skip obsolete files
    json_files = [f for f in json_files if 'obsolete' not in f]
    
    print(f"Found {len(json_files)} questionnaire files to process")
    
    # Combine all linkId mappings
    all_linkids = {}
    
    for filepath in json_files:
        print(f"Processing {Path(filepath).name}...")
        linkid_map = process_questionnaire_file(filepath)
        
        # Add to combined map
        for linkid, text in linkid_map.items():
            # If we have duplicates, prefer the longer/more descriptive text
            if linkid in all_linkids:
                if len(text) > len(all_linkids[linkid]):
                    all_linkids[linkid] = text
            else:
                all_linkids[linkid] = text
    
    # Save the mapping to a JSON file for reference
    output_file = "/Users/mberg/github/engage-analytics/dbt/linkid_question_mapping.json"
    with open(output_file, 'w') as f:
        json.dump(all_linkids, f, indent=2, sort_keys=True)
    
    print(f"\nExtracted {len(all_linkids)} unique linkId-to-question mappings")
    print(f"Saved to {output_file}")
    
    # Show some examples
    examples = list(all_linkids.items())[:10]
    print("\nExample mappings:")
    for linkid, text in examples:
        print(f"  {linkid[:40]}... -> {text[:60]}...")
    
    return all_linkids

if __name__ == "__main__":
    main()