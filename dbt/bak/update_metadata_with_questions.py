#!/usr/bin/env python3
"""
Update metadata with actual question text from questionnaire JSON files
"""
import psycopg2
import csv
import os
import re
import json

# Load the linkId to question mapping
with open('/Users/mberg/github/engage-analytics/dbt/linkid_question_mapping.json', 'r') as f:
    linkid_map = json.load(f)

# Read questionnaire metadata from CSV
questionnaire_map = {}
with open('/Users/mberg/github/engage-analytics/dbt/discovered_questionnaires.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['id'] and row['title']:
            questionnaire_map[row['id']] = row['title']

def create_short_name(column_name, label, table_name=''):
    """Create a shortened column name for use in marts based on the label"""
    # For system fields, keep as is
    system_fields = ['qr_id', 'questionnaire_id', 'subject_patient_id', 
                     'encounter_id', 'author_practitioner_id', 
                     'practitioner_location_id', 'practitioner_organization_id',
                     'practitioner_id', 'practitioner_careteam_id', 
                     'application_version']
    if column_name in system_fields:
        return column_name
    
    # For patient fields, keep as is  
    if column_name.startswith('patient-'):
        return column_name.replace('-', '_')
    
    # For demographic fields
    if column_name.startswith('demographic_'):
        # Shorten common patterns
        short = column_name.replace('demographic_', 'demo_')
        short = short.replace('what_is_your_', '')
        short = short.replace('what_is_the_', '')
        short = short.replace('please_specify', 'specify')
        if len(short) > 30:
            short = short[:30]
        return short
    
    # For UUID fields and others, create meaningful short names from label
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if re.match(uuid_pattern, column_name, re.IGNORECASE) or label:
        if label and len(label) > 3:
            # Clean the label first
            clean_label = label
            
            # Remove common prefixes
            prefixes_to_remove = [
                'Financial Wellness Tool - ',
                'Sociodemographic Survey - ',
                'SBIRT - ',
                'PHQ-9 (IPC Session \\d+) - ',
                'GAD-7 (IPC Session \\d+) - ',
                'Short-Form PCL-5 (IPC Session \\d+) - ',
                'IPC Session \\d+ - ',
                'Mood Rating (IPC Session \\d+) - ',
                'SPI Subform \\d+ - ',
            ]
            
            for prefix in prefixes_to_remove:
                clean_label = re.sub(f'^{prefix}', '', clean_label)
            
            # Special handling for questions
            if 'worried whether my food would run out' in clean_label.lower():
                return 'food_insecurity'
            elif 'pay for the very basics' in clean_label.lower():
                return 'pay_basics_difficulty'
            elif 'not pay the full amount of rent' in clean_label.lower():
                return 'rent_payment_issues'
            elif 'move in with other people' in clean_label.lower():
                return 'moved_financial_reasons'
            elif 'stay at a shelter' in clean_label.lower():
                return 'shelter_stay'
            elif 'run out of money' in clean_label.lower():
                return 'money_ran_out'
            elif 'difficult have these problems made' in clean_label.lower():
                return 'problem_difficulty'
            elif 'little interest or pleasure' in clean_label.lower():
                return 'little_interest'
            elif 'feeling down' in clean_label.lower():
                return 'feeling_down'
            elif 'trouble falling' in clean_label.lower():
                return 'sleep_trouble'
            elif 'tired or having little energy' in clean_label.lower():
                return 'low_energy'
            elif 'poor appetite' in clean_label.lower():
                return 'appetite_problems'
            elif 'feeling bad about yourself' in clean_label.lower():
                return 'feeling_bad_self'
            elif 'trouble concentrating' in clean_label.lower():
                return 'concentration_trouble'
            elif 'moving or speaking' in clean_label.lower():
                return 'psychomotor'
            elif 'better off dead' in clean_label.lower():
                return 'suicidal_thoughts'
            elif 'feeling nervous' in clean_label.lower():
                return 'feeling_nervous'
            elif 'worrying too much' in clean_label.lower():
                return 'excessive_worry'
            elif 'trouble relaxing' in clean_label.lower():
                return 'trouble_relaxing'
            elif 'restless' in clean_label.lower():
                return 'restlessness'
            elif 'easily annoyed' in clean_label.lower():
                return 'irritability'
            elif 'feeling afraid' in clean_label.lower():
                return 'feeling_afraid'
            
            # Extract key words from label
            words = re.findall(r'\b[A-Za-z]+\b', clean_label)
            
            # Filter out common words
            stopwords = ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 
                        'to', 'for', 'of', 'with', 'by', 'from', 'up', 'about',
                        'into', 'through', 'during', 'how', 'when', 'where',
                        'what', 'which', 'who', 'whom', 'this', 'that', 'these',
                        'those', 'then', 'than', 'are', 'was', 'were', 'been',
                        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
                        'could', 'should', 'may', 'might', 'must', 'can', 'much',
                        'often', 'past', 'month', 'months', 'year', 'years', 'time']
            
            meaningful_words = [w.lower() for w in words if w.lower() not in stopwords and len(w) > 2]
            
            if meaningful_words:
                # Take first 3-4 meaningful words
                if len(meaningful_words) >= 3:
                    short = '_'.join(meaningful_words[:3])
                else:
                    short = '_'.join(meaningful_words)
                
                # Ensure uniqueness by adding table context if needed
                if 'sbirt' in table_name and len(short) < 20:
                    short = 'sbirt_' + short
                elif 'financial' in table_name and len(short) < 20:
                    short = 'fin_' + short
                elif 'phq' in table_name and len(short) < 25:
                    short = 'phq_' + short
                elif 'gad' in table_name and len(short) < 25:
                    short = 'gad_' + short
                
                # Limit to 30 characters
                if len(short) > 30:
                    short = short[:30]
                return short
        
        # Fallback: use first 8 chars of UUID
        return f"q_{column_name[:8].replace('-', '')}"
    
    # For fields with dots
    if '.' in column_name:
        return column_name.replace('.', '_')
    
    # For hyphenated fields
    if '-' in column_name:
        if len(column_name) <= 30:
            return column_name.replace('-', '_')
        else:
            # Shorten if too long
            parts = column_name.split('-')
            return '_'.join(parts[:3])[:30]
    
    # Default: return as is if short enough
    if len(column_name) <= 30:
        return column_name
    else:
        return column_name[:30]

def column_to_label(column_name, table_name=''):
    """Convert column names to human-readable labels using actual question text"""
    
    # Special cases for system/generated fields
    special_cases = {
        'qr_id': 'Questionnaire Response ID',
        'questionnaire_id': 'Questionnaire ID',
        'subject_patient_id': 'Subject Patient ID',
        'encounter_id': 'Encounter ID',
        'author_practitioner_id': 'Author Practitioner ID',
        'practitioner_location_id': 'Practitioner Location ID',
        'practitioner_organization_id': 'Practitioner Organization ID',
        'practitioner_id': 'Practitioner ID',
        'practitioner_careteam_id': 'Practitioner Care Team ID',
        'application_version': 'Application Version',
        'patient-age': 'Patient Age',
        'patient-biological-sex': 'Patient Biological Sex',
        'patient-dob': 'Patient Date of Birth',
        'patient-gender-identity': 'Patient Gender Identity',
        'patient-how-you-think-of-yourself': 'Patient How You Think of Yourself',
        'patient-name': 'Patient Name',
        'patient-pronouns': 'Patient Pronouns',
        'task-id-socio-pdf': 'Task ID Socio PDF',
        'some_of_the_time': 'Some of the Time',
    }
    
    if column_name in special_cases:
        return special_cases[column_name]
    
    # Check if it's a UUID and we have question text for it
    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if re.match(uuid_pattern, column_name, re.IGNORECASE):
        # Check if we have question text for this linkId
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:  # Avoid single chars or empty
            # Clean up the text a bit more
            text = question_text.strip()
            # Remove leading commas or periods
            text = re.sub(r'^[,.\s]+', '', text)
            # Limit length
            if len(text) > 80:
                text = text[:77] + "..."
            return text if text else f'Response Field'
        else:
            # Fallback to context-based naming
            if 'sociodemographic' in table_name:
                return 'Demographic Question'
            elif 'phq' in table_name:
                return 'PHQ-9 Item'
            elif 'gad' in table_name:
                return 'GAD-7 Item'
            elif 'spi' in table_name:
                return 'SPI Item'
            elif 'pcl' in table_name:
                return 'PCL-5 Item'
            elif 'mood' in table_name:
                return 'Mood Rating'
            elif 'registration' in table_name:
                return 'Registration Field'
            else:
                return 'Survey Response'
    
    # Handle fields with dots/periods (like f1.26.1)
    if '.' in column_name and re.match(r'^[a-z]\d+\.\d+', column_name):
        # Check if we have this in the linkId map
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:
            text = question_text.strip()
            text = re.sub(r'^[,.\s]+', '', text)
            if len(text) > 80:
                text = text[:77] + "..."
            return text if text else f'Form Question {column_name}'
        else:
            parts = column_name.split('.')
            if len(parts) >= 2:
                return f'Form {parts[0].upper()} Question {parts[1]}'
            return f'Form Question'
    
    # For demographic fields
    if column_name.startswith('demographic_'):
        label = column_name.replace('demographic_', '')
        # Handle specific patterns
        if 'zip_code' in label:
            return 'Zip Code of Current Residence'
        elif 'biological_sex' in label:
            return 'Biological Sex at Birth'
        elif 'race_andor_ethnicity' in label:
            return 'Race and/or Ethnicity'
        elif 'marital_status' in label:
            return 'Marital Status'
        elif 'highest_grade' in label:
            return 'Highest Education Level'
        elif 'where_were_you_born' in label:
            return 'Where Were You Born'
        elif 'not_born_in_the_us' in label:
            return 'If Not Born in US, How Long Have You Lived Here'
        # Generic handling
        label = label.replace('_', ' ')
        return ' '.join(word.capitalize() if word != 'us' else 'US' for word in label.split())
    
    # For IPC session fields
    if column_name.startswith('ipc_s'):
        parts = column_name.split('_')
        if len(parts) > 2 and parts[1].startswith('s'):
            session = parts[1][1:]
            rest = '_'.join(parts[2:]).replace('_', ' ').title()
            return f'IPC Session {session} - {rest}'
        return column_name.replace('ipc_', 'IPC ').replace('_', ' ').title()
    
    # Handle hyphenated fields  
    if '-' in column_name:
        # Check linkId map first
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:
            text = question_text.strip()
            text = re.sub(r'^[,.\s]+', '', text)
            if len(text) > 80:
                text = text[:77] + "..."
            return text if text else column_name.replace('-', ' ').title()
        label = column_name.replace('-', ' ')
        return ' '.join(word.capitalize() for word in label.split())
    
    # Handle numbered fields starting with digits
    if column_name and column_name[0].isdigit():
        label = column_name.replace('_', ' ')
        return label.title()
    
    # Default: replace underscores with spaces and capitalize
    label = column_name.replace('_', ' ')
    return ' '.join(word.capitalize() for word in label.split())

# Connect to database
conn = psycopg2.connect(
    host=os.environ['DBT_HOST'],
    port=os.environ.get('DBT_PORT', 5432),
    database=os.environ['DBT_DATABASE'],
    user=os.environ['DBT_USER'],
    password=os.environ['DBT_PASSWORD']
)
cur = conn.cursor()

# Get all tables
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'engage_analytics_engage_analytics_mart' 
    AND table_name LIKE 'qr_%'
    AND table_name NOT LIKE '%_anon'
    ORDER BY table_name
""")
tables_to_process = [row[0] for row in cur.fetchall()]
cur.close()
conn.close()

# Process each table with a fresh connection
all_columns = []
for table_name in tables_to_process:
    try:
        conn = psycopg2.connect(
            host=os.environ['DBT_HOST'],
            port=os.environ.get('DBT_PORT', 5432),
            database=os.environ['DBT_DATABASE'],
            user=os.environ['DBT_USER'],
            password=os.environ['DBT_PASSWORD']
        )
        cur = conn.cursor()
        
        # Get actual columns
        cur.execute(f"""
            SELECT * FROM engage_analytics_engage_analytics_mart.{table_name} LIMIT 1
        """)
        
        actual_columns = [desc[0] for desc in cur.description]
        
        # Get column data types
        cur.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'engage_analytics_engage_analytics_mart'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        
        column_types = {row[0]: row[1] for row in cur.fetchall()}
        
        # Process each column
        for col_name in actual_columns:
            # Skip problematic columns
            if col_name == 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER':
                continue
                
            data_type = column_types.get(col_name, 'text')
            
            # Determine if PII
            is_pii = False
            pii_patterns = [
                'patient-name', 'patient-dob', 'patient-age', 
                'patient-biological-sex', 'patient-gender-identity',
                'patient-how-you-think-of-yourself', 'patient-pronouns',
                'first_name', 'last_name', 'middle_name',
                'date_of_birth', 'medicaid_number', 'email_address',
                'physical_address', 'phone_number', 'zip_code',
                'unique_id', 'name_family', 'name_given', 'birthdate'
            ]
            
            for pattern in pii_patterns:
                if pattern.lower() in col_name.lower().replace('_', '-'):
                    is_pii = True
                    break
            
            # Determine source
            if col_name.startswith('patient-'):
                source = 'generated'
            elif col_name in ['qr_id', 'questionnaire_id', 'subject_patient_id', 
                              'encounter_id', 'author_practitioner_id', 
                              'practitioner_location_id', 'practitioner_organization_id',
                              'practitioner_id', 'practitioner_careteam_id', 
                              'application_version']:
                source = 'system'
            else:
                source = 'questionnaire'
            
            # Get questionnaire title
            questionnaire_title = ''
            title_mapping = {
                'qr_registration_info': 'Add Family Member Registration',
                'qr_sociodemographic_survey': 'Sociodemographic Survey',
                'qr_spi_subform_1': 'SPI Subform 1',
                'qr_spi_subform_2': 'SPI Subform 2',
                'qr_spi_subform_3': 'SPI Subform 3',
                'qr_spi_subform_4': 'SPI Subform 4',
                'qr_sbirt': 'SBIRT',
                'qr_1_month_follow_up': '1 month follow up',
                'qr_common_mental_health_symptoms': 'Common Mental Health Symptoms',
                'qr_confidential_s1': 'Discuss Confidentiality',
                'qr_financial_wellness_tool': 'Financial Wellness Tool',
                'qr_planning_next_steps': 'Planning Next Steps',
                'qr_mw_tool': 'Mental Wellness Tool',
                'qr_remove_patient': 'eCBIS Remove Family Form',
            }
            
            if table_name in title_mapping:
                questionnaire_title = title_mapping[table_name]
            elif 'gad' in table_name and 's1' in table_name:
                questionnaire_title = 'GAD-7 (IPC Session 1)'
            elif 'gad' in table_name and 's2' in table_name:
                questionnaire_title = 'GAD-7 (IPC Session 2)'
            elif 'gad' in table_name and 's3' in table_name:
                questionnaire_title = 'GAD-7 (IPC Session 3)'
            elif 'gad' in table_name and 's4' in table_name:
                questionnaire_title = 'GAD-7 (IPC Session 4)'
            elif 'phq' in table_name and 's1' in table_name:
                questionnaire_title = 'PHQ-9 (IPC Session 1)'
            elif 'phq' in table_name and 's2' in table_name:
                questionnaire_title = 'PHQ-9 (IPC Session 2)'
            elif 'phq' in table_name and 's3' in table_name:
                questionnaire_title = 'PHQ-9 (IPC Session 3)'
            elif 'phq' in table_name and 's4' in table_name:
                questionnaire_title = 'PHQ-9 (IPC Session 4)'
            elif 'mood_rating' in table_name and 's1' in table_name:
                questionnaire_title = 'Mood Rating (IPC Session 1)'
            elif 'mood_rating' in table_name and 's2' in table_name:
                questionnaire_title = 'Mood Rating (IPC Session 2)'
            elif 'mood_rating' in table_name and 's3' in table_name:
                questionnaire_title = 'Mood Rating (IPC Session 3)'
            elif 'mood_rating' in table_name and 's4' in table_name:
                questionnaire_title = 'Mood Rating (IPC Session 4)'
            elif 'pcl' in table_name and 's1' in table_name:
                questionnaire_title = 'Short-Form PCL-5-8 (IPC Session 1)'
            elif 'pcl' in table_name and 's2' in table_name:
                questionnaire_title = 'Short-Form PCL-5-8 (IPC Session 2)'
            elif 'pcl' in table_name and 's3' in table_name:
                questionnaire_title = 'Short-Form PCL-5-8 (IPC Session 3)'
            elif 'pcl' in table_name and 's4' in table_name:
                questionnaire_title = 'Short-Form PCL-5-8 (IPC Session 4)'
            elif 'start_ipc' in table_name and 's1' in table_name:
                questionnaire_title = 'IPC Session 1'
            elif 'start_ipc' in table_name and 's2' in table_name:
                questionnaire_title = 'IPC Session 2'
            elif 'start_ipc' in table_name and 's3' in table_name:
                questionnaire_title = 'IPC Session 3'
            elif 'start_ipc' in table_name and 's4' in table_name:
                questionnaire_title = 'IPC Session 4'
            
            # Generate human-readable label using actual question text
            label = column_to_label(col_name, table_name)
            
            # Generate shortened column name for marts
            short_name = create_short_name(col_name, label, table_name)
            
            all_columns.append({
                'table': table_name,
                'column': col_name,
                'short_name': short_name,
                'label': label,
                'data_type': data_type,
                'questionnaire_title': questionnaire_title,
                'source': source,
                'anon': 'TRUE' if is_pii else 'FALSE'
            })
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Warning: Could not process table {table_name}: {e}")
        if conn:
            conn.close()
        continue

# Write to CSV with improved labels and short names
with open('/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['table', 'column', 'short_name', 'label', 'data_type', 'questionnaire_title', 'source', 'anon'])
    writer.writeheader()
    writer.writerows(all_columns)

print(f"Updated metadata with {len(all_columns)} fields with actual question text")
print(f"Tables processed: {len(tables_to_process)}")
print(f"PII fields marked: {sum(1 for c in all_columns if c['anon'] == 'TRUE')}")

# Show examples
print(f"\nExample improved labels:")
# UUIDs with actual question text
uuid_examples = [c for c in all_columns if re.match(r'^[0-9a-f]{8}-', c['column']) and not c['label'].startswith('Survey')][:10]
for ex in uuid_examples:
    print(f"  {ex['column'][:30]}... -> {ex['label'][:60]}...")

print(f"\nSummary statistics:")
print(f"  Fields with actual question text: {sum(1 for c in all_columns if not any(c['label'].startswith(x) for x in ['Survey', 'PHQ-9', 'GAD-7', 'Form', 'Demographic Question', 'PCL-5', 'SPI', 'Mood', 'Registration']))}")
print(f"  Generic labels: {sum(1 for c in all_columns if any(c['label'].startswith(x) for x in ['Survey', 'PHQ-9 Item', 'GAD-7 Item', 'Form Question', 'Demographic Question', 'PCL-5 Item', 'SPI Item', 'Mood Rating', 'Registration Field']))}")