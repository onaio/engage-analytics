#!/usr/bin/env python3
import json
import os
import csv
import re
from pathlib import Path

def extract_uuid_linkids(item, results=None):
    """Recursively extract UUID linkIds and their text from questionnaire items"""
    if results is None:
        results = []
    
    # Check if this item has a UUID linkId
    if 'linkId' in item:
        linkid = item['linkId']
        # Check if it's a UUID format
        if re.match(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$', linkid):
            text = item.get('text', '')
            results.append({
                'linkid': linkid,
                'text': text
            })
    
    # Recursively process nested items
    if 'item' in item:
        for nested_item in item['item']:
            extract_uuid_linkids(nested_item, results)
    
    return results

def process_questionnaire_file(file_path):
    """Process a single questionnaire file to extract UUID linkIds"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Get questionnaire ID
        qid = data.get('id', '')
        if qid.startswith('ipc-'):
            questionnaire_id = f"Questionnaire/{qid.replace('ipc-', '')}"
        else:
            questionnaire_id = f"Questionnaire/{qid}"
        
        # Extract all UUID linkIds
        uuid_items = []
        if 'item' in data:
            for item in data['item']:
                extract_uuid_linkids(item, uuid_items)
        
        return questionnaire_id, uuid_items
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, []

def clean_text_for_alias(text):
    """Convert text to a clean alias"""
    if not text:
        return "field"
    
    # Take first 60 chars to avoid overly long aliases
    text = text[:60]
    
    # Remove special characters and convert to snake_case
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '_', text)
    text = text.lower()
    
    # Remove trailing underscores
    text = text.rstrip('_')
    
    return text if text else "field"

# Process all questionnaire files
questionnaire_dir = Path('/Users/mberg/github/engage-analytics/questionnaire')
all_uuid_metadata = []

for json_file in questionnaire_dir.rglob('*.json'):
    qid, uuid_items = process_questionnaire_file(json_file)
    if qid and uuid_items:
        for item in uuid_items:
            all_uuid_metadata.append({
                'questionnaire_id': qid,
                'linkid': item['linkid'],
                'text': item['text'],
                'file': str(json_file.relative_to(questionnaire_dir))
            })

print(f"Found {len(all_uuid_metadata)} UUID linkIds in questionnaire files")

# Group by linkId to see patterns
from collections import defaultdict
linkid_texts = defaultdict(set)
for item in all_uuid_metadata:
    if item['text']:
        linkid_texts[item['linkid']].add(item['text'])

# Create consistent aliases for each UUID
uuid_aliases = {}
for linkid, texts in linkid_texts.items():
    if texts:
        # Use the most common or first text
        text = list(texts)[0]
        alias = clean_text_for_alias(text)
        uuid_aliases[linkid] = {
            'text': text,
            'alias': alias
        }

# Read existing metadata
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
existing_data = []
existing_keys = set()

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        existing_data.append(row)
        existing_keys.add((row['questionnaire_id'], row['linkid']))

# Read unmapped fields from our analysis
unmapped_file = '/Users/mberg/github/engage-analytics/dbt/unmapped_uuid_fields.csv'
unmapped_fields = []
with open(unmapped_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        unmapped_fields.append(row)

# Create new metadata entries for unmapped UUID fields
new_entries = []
order_offset = 1000

for field in unmapped_fields:
    qid = field['questionnaire_id']
    linkid = field['linkid']
    
    # Skip if already exists
    if (qid, linkid) in existing_keys:
        continue
    
    # Get alias and text from our extraction or use defaults
    if linkid in uuid_aliases:
        text = uuid_aliases[linkid]['text']
        alias = uuid_aliases[linkid]['alias']
    else:
        # Use sample value as hint for text if no text found
        sample = field.get('sample_value', '')
        if sample and sample != '[empty]':
            text = f"Field ({sample[:30]}...)" if len(sample) > 30 else f"Field ({sample})"
            alias = clean_text_for_alias(sample)
        else:
            text = "UUID Field"
            alias = f"field_{linkid[:8]}"
    
    new_entry = {
        'questionnaire_id': qid,
        'linkid': linkid,
        'question_text': text[:100],  # Limit text length
        'question_alias': alias[:63],  # PostgreSQL column name limit
        'question_order': str(order_offset)
    }
    new_entries.append(new_entry)
    order_offset += 1

print(f"\nPreparing to add {len(new_entries)} new metadata entries")

# Combine all data
all_data = existing_data + new_entries
all_data.sort(key=lambda x: (x['questionnaire_id'], int(x['question_order']) if x['question_order'] else 999))

# Write back to file
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['questionnaire_id', 'linkid', 'question_text', 'question_alias', 'question_order']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)

print(f"Updated metadata file with {len(all_data)} total entries")

# Show summary of additions
questionnaire_counts = defaultdict(int)
for entry in new_entries:
    questionnaire_counts[entry['questionnaire_id']] += 1

print("\nNew entries added by questionnaire:")
for qid in sorted(questionnaire_counts.keys()):
    print(f"  {qid}: {questionnaire_counts[qid]} fields")