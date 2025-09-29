#!/usr/bin/env python3
"""
Flask application for secure data export with authentication and audit logging
"""

import os
import json
import time
import zipfile
import tempfile
from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect, url_for, 
    session, flash, send_file, Response, jsonify
)
from werkzeug.security import check_password_hash, generate_password_hash
from dotenv import load_dotenv

from export_handler import DataExporter

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 hour
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
app.config['SESSION_COOKIE_SECURE'] = False  # Set to True in production with HTTPS
app.config['SESSION_COOKIE_HTTPONLY'] = True

# Authentication passwords
AUTH_PASSWORD_ANON = os.getenv('AUTH_PASSWORD_ANON', 'secure_password_123')
AUTH_PASSWORD_PII = os.getenv('AUTH_PASSWORD_PII', 'pii_secure_password_456')
PASSWORD_HASH_ANON = generate_password_hash(AUTH_PASSWORD_ANON)
PASSWORD_HASH_PII = generate_password_hash(AUTH_PASSWORD_PII)

# Ensure downloads.log exists
LOG_FILE = Path(__file__).parent / 'downloads.log'
LOG_FILE.touch(exist_ok=True)

# Global variable to track export progress
export_progress = {}


def login_required(f):
    """Decorator to require login for routes"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            flash('Please login to access this page.', 'warning')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


def log_activity(email, ip_address, action='export_started', status='success', error_msg=None):
    """Log all export activities to file"""
    timestamp = datetime.now().isoformat()
    log_entry = {
        'timestamp': timestamp,
        'email': email,
        'ip_address': ip_address,
        'action': action,
        'status': status,
        'error': error_msg
    }
    
    with open(LOG_FILE, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')


@app.route('/')
def index():
    """Redirect to login or download page based on auth status"""
    if session.get('authenticated'):
        return redirect(url_for('download_form'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    """Login page with password authentication"""
    if request.method == 'POST':
        password = request.form.get('password', '')

        # Check password to determine access level
        has_pii_access = check_password_hash(PASSWORD_HASH_PII, password)
        has_anon_access = check_password_hash(PASSWORD_HASH_ANON, password)

        if has_pii_access:
            session['authenticated'] = True
            session['access_level'] = 'pii'
            session.permanent = False  # Session expires when browser closes
            flash('Successfully logged in with full access!', 'success')
            return redirect(url_for('download_form'))
        elif has_anon_access:
            session['authenticated'] = True
            session['access_level'] = 'anon'
            session.permanent = False  # Session expires when browser closes
            flash('Successfully logged in with anonymized data access.', 'success')
            return redirect(url_for('download_form'))
        else:
            flash('Invalid password. Please try again.', 'danger')

    return render_template('login.html')


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    """Logout and clear session"""
    email = session.get('export_email', 'unknown')
    ip_address = request.remote_addr

    # Log logout activity
    if session.get('authenticated'):
        log_activity(email, ip_address, action='logout', status='success')

    session.clear()

    # Handle beacon requests (don't redirect)
    if request.method == 'POST' or request.headers.get('Content-Type') == 'text/plain':
        return '', 204  # No content response for beacon

    flash('You have been logged out for security.', 'info')
    return redirect(url_for('login'))


@app.route('/download', methods=['GET', 'POST'])
@login_required
def download_form():
    """Download form page to collect email before export"""
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        export_type = request.form.get('export_type', 'anon')

        # Verify user has access to requested export type
        access_level = session.get('access_level', 'anon')
        if export_type == 'pii' and access_level != 'pii':
            flash('Access denied. PII export requires elevated access.', 'danger')
            return render_template('download.html')

        if not email or '@' not in email:
            flash('Please provide a valid email address.', 'warning')
            return render_template('download.html')

        # Store export type in session
        session['export_type'] = export_type

        # Log export request
        ip_address = request.remote_addr
        export_label = 'PII' if export_type == 'pii' else 'ANON'
        log_activity(email, ip_address, action=f'export_requested_{export_label}', status='success')

        # Store email in session for export process
        session['export_email'] = email
        return redirect(url_for('prepare_export'))

    return render_template('download.html')


@app.route('/prepare-export')
@login_required
def prepare_export():
    """Page that shows progress while preparing export"""
    email = session.get('export_email')
    if not email:
        flash('Please provide your email first.', 'warning')
        return redirect(url_for('download_form'))
    
    # Generate unique export ID
    export_id = f"export_{int(time.time())}"
    session['export_id'] = export_id
    
    return render_template('progress.html', export_id=export_id)


@app.route('/export-progress/<export_id>')
@login_required
def export_progress_stream(export_id):
    """Server-sent events endpoint for progress updates"""
    # Get email and export type from session before entering generator
    email = session.get('export_email', 'unknown')
    export_type = session.get('export_type', 'anon')
    ip_address = request.remote_addr

    def generate():
        """Generate server-sent events"""
        # Initialize progress
        export_progress[export_id] = {'status': 'starting', 'progress': 0, 'message': 'Initializing export...'}

        # Create a timestamp-based folder for this export in the dataexport directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        exports_base = Path('/Users/mberg/github/engage-analytics/dataexport/exports')
        exports_base.mkdir(exist_ok=True)
        export_dir = exports_base / f"export_{timestamp}_{export_type}"
        export_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Log export started
            export_label = 'PII' if export_type == 'pii' else 'ANON'
            log_activity(email, ip_address, action=f'export_started_{export_label}', status='success')

            # Create exporter instance
            exporter = DataExporter()

            # Connect to database
            yield f"data: {json.dumps({'progress': 5, 'message': 'Connecting to database...'})}\n\n"
            exporter.connect()

            # Get list of tables to export based on type
            yield f"data: {json.dumps({'progress': 10, 'message': 'Fetching table list...'})}\n\n"
            if export_type == 'pii':
                tables = exporter.get_pii_tables()
            else:
                tables = exporter.get_anonymized_tables()
            total_tables = len(tables)
            
            # Export each table to disk
            for i, table in enumerate(tables):
                progress_pct = 10 + int((i / total_tables) * 75)
                yield f"data: {json.dumps({'progress': progress_pct, 'message': f'Exporting {table}... ({i+1}/{total_tables})'})}\n\n"
                
                csv_file = export_dir / f"{table}.csv"
                exporter.export_table_to_csv(table, csv_file)
            
            # Create zip file
            yield f"data: {json.dumps({'progress': 90, 'message': 'Creating zip archive...'})}\n\n"

            export_label = 'pii' if export_type == 'pii' else 'anon'
            zip_filename = f"engage_analytics_export_{export_label}_{timestamp}.zip"
            zip_path = export_dir / zip_filename
            
            # Create the zip file without sending progress updates during compression
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                csv_files = list(export_dir.glob('*.csv'))
                for csv_file in csv_files:
                    zipf.write(csv_file, csv_file.name)
            
            # Verify zip file was created
            if not zip_path.exists():
                raise Exception(f"Failed to create zip file at {zip_path}")
            
            # Log zip creation
            print(f"DEBUG: Zip file created at {zip_path} (size: {zip_path.stat().st_size} bytes)")
            
            # Zip is complete - update to 95%
            yield f"data: {json.dumps({'progress': 95, 'message': 'Zip file created successfully!'})}\n\n"
            
            # Small delay to ensure message is sent
            time.sleep(0.5)
            
            # Store zip path in session for download
            session['export_file'] = str(zip_path)
            export_progress[export_id] = {
                'status': 'complete',
                'progress': 100,
                'message': 'Export ready for download!',
                'file_path': str(zip_path),
                'export_dir': str(export_dir)
            }
            
            # Log successful export completion
            export_label = 'PII' if export_type == 'pii' else 'ANON'
            log_activity(email, ip_address, action=f'export_completed_{export_label}', status='success')
            
            # Send final completion message with download ready
            yield f"data: {json.dumps({'progress': 100, 'message': 'Export complete! Click Download to get your file.', 'complete': True})}\n\n"
            
            # Final flush to ensure all messages are sent
            time.sleep(0.5)
            
        except Exception as e:
            error_msg = f"Export failed: {str(e)}"
            export_progress[export_id] = {'status': 'error', 'progress': 0, 'message': error_msg}
            yield f"data: {json.dumps({'error': True, 'message': error_msg})}\n\n"
            # DO NOT DELETE - Keep all exports for debugging
        
        finally:
            exporter.disconnect()
    
    return Response(generate(), mimetype='text/event-stream')


@app.route('/download-file')
@login_required
def download_file():
    """Download the prepared export file"""
    export_id = session.get('export_id')
    email = session.get('export_email')
    
    print(f"DEBUG: Download requested - export_id: {export_id}, email: {email}")
    
    # First try to get from export_progress
    if export_id and export_id in export_progress:
        export_info = export_progress[export_id]
        file_path = export_info.get('file_path')
        if file_path and Path(file_path).exists():
            print(f"DEBUG: Found file from export_progress: {file_path}")
            # Log successful download
            ip_address = request.remote_addr
            log_activity(email or 'unknown', ip_address, action='file_downloaded', status='success')
            
            response = send_file(
                file_path,
                as_attachment=True,
                download_name=Path(file_path).name,
                mimetype='application/zip'
            )
            return response
    
    # Fallback: Find the most recent zip file in exports directory
    exports_base = Path('/Users/mberg/github/engage-analytics/dataexport/exports')
    if exports_base.exists():
        zip_files = list(exports_base.glob('*/*.zip'))
        if zip_files:
            # Get the most recent zip file
            latest_zip = max(zip_files, key=lambda p: p.stat().st_mtime)
            print(f"DEBUG: Using latest zip file: {latest_zip}")
            
            # Log successful download
            ip_address = request.remote_addr
            log_activity(email or 'unknown', ip_address, action='file_downloaded', status='success')
            
            response = send_file(
                str(latest_zip),
                as_attachment=True,
                download_name=latest_zip.name,
                mimetype='application/zip'
            )
            return response
    
    flash('No export file found. Please try again.', 'danger')
    return redirect(url_for('download_form'))
    
    # Optional: Schedule cleanup of export directory after sending file
    # Disabled for now so you can inspect the files
    # def cleanup_export():
    #     try:
    #         export_dir = export_info.get('export_dir')
    #         if export_dir and Path(export_dir).exists():
    #             import shutil
    #             shutil.rmtree(export_dir, ignore_errors=True)
    #         # Remove from progress tracking
    #         export_progress.pop(export_id, None)
    #     except Exception:
    #         pass  # Ignore cleanup errors
    
    # Send file
    response = send_file(
        file_path,
        as_attachment=True,
        download_name=Path(file_path).name,
        mimetype='application/zip'
    )
    
    # Schedule cleanup after response (disabled)
    # from threading import Timer
    # Timer(5.0, cleanup_export).start()
    
    return response


@app.route('/download-latest')
@login_required
def download_latest():
    """Direct download of the latest export file"""
    exports_base = Path('/Users/mberg/github/engage-analytics/dataexport/exports')
    if exports_base.exists():
        zip_files = list(exports_base.glob('*/*.zip'))
        if zip_files:
            # Get the most recent zip file
            latest_zip = max(zip_files, key=lambda p: p.stat().st_mtime)
            print(f"DEBUG: Downloading latest zip: {latest_zip}")
            
            response = send_file(
                str(latest_zip),
                as_attachment=True,
                download_name=latest_zip.name,
                mimetype='application/zip'
            )
            return response
    
    flash('No export files available.', 'danger')
    return redirect(url_for('download_form'))


@app.route('/api/export-status/<export_id>')
@login_required
def api_export_status(export_id):
    """API endpoint to check export status"""
    if export_id not in export_progress:
        return jsonify({'status': 'not_found'}), 404
    
    export_info = export_progress[export_id]
    response = {
        'status': export_info.get('status'),
        'progress': export_info.get('progress'),
        'message': export_info.get('message')
    }
    
    # If complete, add download readiness
    if export_info.get('status') == 'complete':
        file_path = export_info.get('file_path')
        if file_path and Path(file_path).exists():
            response['download_ready'] = True
            response['file_size'] = Path(file_path).stat().st_size
        else:
            response['download_ready'] = False
    
    return jsonify(response)


@app.route('/api/tables')
@login_required
def api_tables():
    """API endpoint to get list of tables that will be exported"""
    try:
        export_type = request.args.get('export_type', session.get('export_type', 'anon'))
        exporter = DataExporter()
        exporter.connect()
        if export_type == 'pii':
            tables = exporter.get_pii_tables()
        else:
            tables = exporter.get_anonymized_tables()
        exporter.disconnect()
        return jsonify({'tables': tables, 'count': len(tables), 'export_type': export_type})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    print("Starting Flask Data Export Application")
    print(f"Access the application at: http://localhost:{port}")
    print(f"Anonymized data password: {AUTH_PASSWORD_ANON}")
    print(f"PII data password: {AUTH_PASSWORD_PII}")
    print("-" * 50)
    app.run(debug=True, host='0.0.0.0', port=port)