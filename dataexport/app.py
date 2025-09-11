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

# Authentication password
AUTH_PASSWORD = os.getenv('AUTH_PASSWORD', 'secure_password_123')
PASSWORD_HASH = generate_password_hash(AUTH_PASSWORD)

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
        
        if check_password_hash(PASSWORD_HASH, password):
            session['authenticated'] = True
            session.permanent = True
            flash('Successfully logged in!', 'success')
            return redirect(url_for('download_form'))
        else:
            flash('Invalid password. Please try again.', 'danger')
    
    return render_template('login.html')


@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    flash('You have been logged out.', 'info')
    return redirect(url_for('login'))


@app.route('/download', methods=['GET', 'POST'])
@login_required
def download_form():
    """Download form page to collect email before export"""
    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        
        if not email or '@' not in email:
            flash('Please provide a valid email address.', 'warning')
            return render_template('download.html')
        
        # Log export request
        ip_address = request.remote_addr
        log_activity(email, ip_address, action='export_requested', status='success')
        
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
    def generate():
        """Generate server-sent events"""
        # Initialize progress
        export_progress[export_id] = {'status': 'starting', 'progress': 0, 'message': 'Initializing export...'}
        
        # Create a timestamp-based folder for this export in the dataexport directory
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        exports_base = Path('/Users/mberg/github/engage-analytics/dataexport/exports')
        exports_base.mkdir(exist_ok=True)
        export_dir = exports_base / f"export_{timestamp}"
        export_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Create exporter instance
            exporter = DataExporter()
            
            # Connect to database
            yield f"data: {json.dumps({'progress': 5, 'message': 'Connecting to database...'})}\n\n"
            exporter.connect()
            
            # Get list of tables to export
            yield f"data: {json.dumps({'progress': 10, 'message': 'Fetching table list...'})}\n\n"
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
            
            zip_filename = f"engage_analytics_export_{timestamp}.zip"
            zip_path = export_dir / zip_filename
            
            # Create the zip file without sending progress updates during compression
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                csv_files = list(export_dir.glob('*.csv'))
                for csv_file in csv_files:
                    zipf.write(csv_file, csv_file.name)
            
            # Zip is complete - update to 100%
            yield f"data: {json.dumps({'progress': 95, 'message': 'Zip file created successfully!'})}\n\n"
            
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
            email = session.get('export_email', 'unknown')
            ip_address = request.remote_addr if request else 'unknown'
            log_activity(email, ip_address, action='export_completed', status='success')
            
            # Send final completion message with download ready
            yield f"data: {json.dumps({'progress': 100, 'message': 'Export complete! Click Download to get your file.', 'complete': True})}\n\n"
            
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
    
    if not export_id or export_id not in export_progress:
        flash('No export available. Please start a new export.', 'warning')
        return redirect(url_for('download_form'))
    
    export_info = export_progress[export_id]
    if export_info['status'] != 'complete':
        flash('Export is not ready yet. Please wait.', 'warning')
        return redirect(url_for('prepare_export'))
    
    file_path = export_info.get('file_path')
    if not file_path or not Path(file_path).exists():
        flash('Export file not found. Please try again.', 'danger')
        return redirect(url_for('download_form'))
    
    # Log successful download
    ip_address = request.remote_addr
    log_activity(email, ip_address, action='file_downloaded', status='success')
    
    # Clean up session
    session.pop('export_id', None)
    session.pop('export_email', None)
    
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


@app.route('/api/tables')
@login_required
def api_tables():
    """API endpoint to get list of tables that will be exported"""
    try:
        exporter = DataExporter()
        exporter.connect()
        tables = exporter.get_anonymized_tables()
        exporter.disconnect()
        return jsonify({'tables': tables, 'count': len(tables)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('FLASK_PORT', 5001))
    print("Starting Flask Data Export Application")
    print(f"Access the application at: http://localhost:{port}")
    print(f"Default password: {AUTH_PASSWORD}")
    print("-" * 50)
    app.run(debug=True, host='0.0.0.0', port=port)