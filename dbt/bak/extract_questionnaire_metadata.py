#!/usr/bin/env python3
import json
import csv
import os
import re
from pathlib import Path

def clean_alias(text, prefix="", index=0):
    """Convert text to a clean column alias"""
    if not text:
        return f"{prefix}_field_{index}" if prefix else f"field_{index}"
    
    # Remove special characters and convert to snake_case
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', '_', text)
    text = text.lower()[:50]  # Limit length
    
    if prefix:
        return f"{prefix}_{text}"
    return text

def extract_linkid_text(item, prefix="", results=None, order_counter=None):
    """Recursively extract linkId and text from questionnaire items"""
    if results is None:
        results = []
    if order_counter is None:
        order_counter = {'count': 0}
    
    linkid = item.get('linkId', '')
    text = item.get('text', '')
    
    if linkid:
        order_counter['count'] += 1
        # Clean up the text for display
        if text:
            text = text.replace('\n', ' ').replace('\r', ' ')
            text = re.sub(r'\[.*?\]', '', text)  # Remove variable placeholders
            text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
            text = re.sub(r'\s+', ' ', text).strip()
        
        results.append({
            'linkid': linkid,
            'text': text[:200] if text else '',  # Limit text length
            'order': order_counter['count']
        })
    
    # Recursively process nested items
    if 'item' in item:
        for subitem in item['item']:
            extract_linkid_text(subitem, prefix, results, order_counter)
    
    return results

def process_questionnaire_file(file_path, questionnaire_id):
    """Process a single questionnaire JSON file"""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Get questionnaire title for prefix
        title = data.get('title', '')
        prefix = clean_alias(title)
        
        # Map common questionnaire types to shorter prefixes
        prefix_map = {
            'phq_9': 'phq9',
            'gad_7': 'gad7',
            'short_form_pcl_5_8': 'pcl5',
            'mood_rating': 'mood',
            'sbirt': 'sbirt',
            'spi_subform': 'spi',
            'financial_wellness_tool': 'financial',
            'planning_next_steps': 'planning',
            'mental_wellness_tool': 'mental',
            'common_mental_health_symptoms': 'mental_symptoms',
            'sociodemographic_survey': 'demographic',
            'ipc_session': 'ipc',
            'discuss_confidentiality': 'confidential',
            'add_family_member_registration': 'registration',
            'ecbis_remove_family_form': 'remove_family'
        }
        
        for key, value in prefix_map.items():
            if key in prefix:
                prefix = value
                break
        
        # Add session number if applicable
        if 'Session' in title:
            session_match = re.search(r'Session (\d+)', title)
            if session_match:
                prefix = f"{prefix}_s{session_match.group(1)}"
        
        results = []
        
        # Process all items in the questionnaire
        if 'item' in data:
            for item in data['item']:
                extract_linkid_text(item, prefix, results)
        
        # Add questionnaire_id to each result
        for i, result in enumerate(results):
            result['questionnaire_id'] = questionnaire_id
            # Create a clean alias for each field
            if result['text']:
                # Special handling for common question types
                text_lower = result['text'].lower()
                if 'little interest' in text_lower:
                    result['alias'] = f"{prefix}_q1_interest"
                elif 'feeling down' in text_lower:
                    result['alias'] = f"{prefix}_q2_depressed"
                elif 'trouble' in text_lower and 'sleep' in text_lower:
                    result['alias'] = f"{prefix}_q3_sleep"
                elif 'feeling tired' in text_lower:
                    result['alias'] = f"{prefix}_q4_tired"
                elif 'poor appetite' in text_lower:
                    result['alias'] = f"{prefix}_q5_appetite"
                elif 'feeling bad about yourself' in text_lower:
                    result['alias'] = f"{prefix}_q6_failure"
                elif 'trouble concentrating' in text_lower:
                    result['alias'] = f"{prefix}_q7_concentration"
                elif 'moving' in text_lower and 'slowly' in text_lower:
                    result['alias'] = f"{prefix}_q8_movement"
                elif 'thoughts' in text_lower and ('dead' in text_lower or 'hurt' in text_lower):
                    result['alias'] = f"{prefix}_q9_suicide"
                elif 'total score' in text_lower and 'meaning' in text_lower:
                    result['alias'] = f"{prefix}_score_meaning"
                elif 'total score' in text_lower:
                    result['alias'] = f"{prefix}_total_score"
                elif 'nervous' in text_lower or 'anxious' in text_lower:
                    result['alias'] = f"{prefix}_q1_anxious"
                elif 'control' in text_lower and 'worry' in text_lower:
                    result['alias'] = f"{prefix}_q2_control_worry"
                elif 'worrying' in text_lower and 'different' in text_lower:
                    result['alias'] = f"{prefix}_q3_worry_too_much"
                elif 'trouble relaxing' in text_lower:
                    result['alias'] = f"{prefix}_q4_trouble_relaxing"
                elif 'restless' in text_lower:
                    result['alias'] = f"{prefix}_q5_restless"
                elif 'annoyed' in text_lower or 'irritable' in text_lower:
                    result['alias'] = f"{prefix}_q6_irritable"
                elif 'afraid' in text_lower:
                    result['alias'] = f"{prefix}_q7_afraid"
                else:
                    result['alias'] = clean_alias(result['text'], prefix, i+1)
            else:
                # For fields without text, use the linkId
                result['alias'] = clean_alias(result['linkid'], prefix, i+1)
        
        return results
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return []

def main():
    # Read the discovered questionnaires
    discovered_file = '/Users/mberg/github/engage-analytics/dbt/discovered_questionnaires.csv'
    questionnaire_base = '/Users/mberg/github/engage-analytics'
    
    all_metadata = []
    
    # Keep existing PHQ-9 metadata (Questionnaire/54)
    existing_metadata = []
    existing_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
    if os.path.exists(existing_file):
        with open(existing_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['questionnaire_id'] == 'Questionnaire/54':
                    existing_metadata.append(row)
    
    with open(discovered_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip PHQ-9 since we already have it
            if row['id'] == 'ipc-54':
                continue
                
            # Build questionnaire ID
            if row['id'].startswith('ipc-'):
                # Remove 'ipc-' prefix for numeric IDs
                qid = row['id'].replace('ipc-', '')
                questionnaire_id = f"Questionnaire/{qid}"
            else:
                questionnaire_id = f"Questionnaire/{row['id']}"
            
            # Build full file path
            file_path = os.path.join(questionnaire_base, row['path'])
            
            if os.path.exists(file_path):
                print(f"Processing {row['title']} ({questionnaire_id})")
                metadata = process_questionnaire_file(file_path, questionnaire_id)
                all_metadata.extend(metadata)
            else:
                print(f"File not found: {file_path}")
    
    # Write to CSV
    output_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata_full.csv'
    
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['questionnaire_id', 'linkid', 'question_text', 'question_alias', 'question_order']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write existing PHQ-9 metadata first
        for row in existing_metadata:
            writer.writerow({
                'questionnaire_id': row['questionnaire_id'],
                'linkid': row['linkid'],
                'question_text': row['question_text'],
                'question_alias': row['question_alias'],
                'question_order': row['question_order']
            })
        
        # Write new metadata
        for item in all_metadata:
            writer.writerow({
                'questionnaire_id': item['questionnaire_id'],
                'linkid': item['linkid'],
                'question_text': item['text'],
                'question_alias': item['alias'],
                'question_order': item['order']
            })
    
    print(f"\nMetadata extracted to {output_file}")
    print(f"Total fields extracted: {len(all_metadata)}")

if __name__ == "__main__":
    main()