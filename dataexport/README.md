# Engage Analytics Data Export Web Application

A secure Flask web application for exporting anonymized healthcare data from the Engage Analytics database.

## Features

- **Password Protection**: Static password authentication to secure access
- **User Audit Trail**: Logs email and timestamp for every download
- **Real-time Progress**: Shows export progress with Server-Sent Events
- **Anonymized Data Only**: Exports only de-identified data from `*_anon` views
- **On-Demand Generation**: Data is exported fresh each time (not cached)
- **Bulk Download**: All tables exported as CSV files in a single ZIP archive

## Installation

1. Install Python dependencies:
```bash
cd dataexport
pip install -r requirements.txt
```

2. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials and password
```

3. Ensure database access:
- The application needs access to the PostgreSQL database
- Database credentials are inherited from the parent `.envrc` file

## Running the Application

### Development Mode
```bash
python app.py
```
The application will be available at `http://localhost:5000`

### Production Mode
For production, use a WSGI server like Gunicorn:
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Usage

1. **Login**: Navigate to the application and enter the password
2. **Provide Email**: Enter your email address for audit logging
3. **Confirm Consent**: Check the consent box to proceed
4. **Start Export**: Click "Start Export Process"
5. **Monitor Progress**: Watch real-time progress as tables are exported
6. **Download**: Once complete, download the ZIP file

## Configuration

### Environment Variables

- `AUTH_PASSWORD`: The password for accessing the application
- `SECRET_KEY`: Flask secret key for session management
- `DBT_HOST`, `DBT_DATABASE`, etc.: Database connection parameters

### Security Considerations

- Always use HTTPS in production
- Change the default password immediately
- Use a strong, unique `SECRET_KEY`
- Regularly rotate passwords
- Monitor the `downloads.log` file for unauthorized access

## Data Exported

The application exports only anonymized data from:
- `qr_*_anon` views (questionnaire responses)
- `patient_anon` table (de-identified patient data)
- Other resource tables (practitioners, organizations, encounters)

All personally identifiable information (PII) is excluded or masked.

## Audit Logging

All downloads are logged to `downloads.log` with:
- Timestamp
- User email
- IP address
- Status (success/failure)
- Error messages (if any)

## Troubleshooting

### Database Connection Issues
- Verify database credentials in `.env`
- Ensure PostgreSQL is running
- Check network connectivity

### Export Failures
- Check available disk space
- Verify database permissions
- Review error messages in the web interface

### Performance Issues
- Large exports may take several minutes
- Consider implementing pagination for very large tables
- Monitor database load during exports

## Development

### Testing the Export Handler
```bash
python export_handler.py
# To test actual export:
python export_handler.py --export
```

### File Structure
```
dataexport/
├── app.py                 # Main Flask application
├── export_handler.py      # Database export logic
├── templates/            # HTML templates
├── static/              # CSS and JavaScript
├── requirements.txt     # Python dependencies
├── .env                # Environment configuration
└── downloads.log       # Audit log file
```

## License

This application is part of the Engage Analytics project and follows the same licensing terms.