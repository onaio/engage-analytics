// Engage Analytics Data Export - Client-side JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Auto-dismiss alerts after 5 seconds
    const alerts = document.querySelectorAll('.alert:not(.alert-info)');
    alerts.forEach(alert => {
        setTimeout(() => {
            const bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        }, 5000);
    });
    
    // Form validation
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', function(event) {
            if (!form.checkValidity()) {
                event.preventDefault();
                event.stopPropagation();
            }
            form.classList.add('was-validated');
        });
    });
    
    // Password visibility toggle
    const passwordInput = document.getElementById('password');
    if (passwordInput) {
        const toggleButton = document.createElement('button');
        toggleButton.type = 'button';
        toggleButton.className = 'btn btn-sm btn-outline-secondary position-absolute end-0 top-50 translate-middle-y me-2';
        toggleButton.style.zIndex = '10';
        toggleButton.innerHTML = '<i class="fas fa-eye"></i>';
        
        passwordInput.parentElement.style.position = 'relative';
        passwordInput.parentElement.appendChild(toggleButton);
        
        toggleButton.addEventListener('click', function() {
            if (passwordInput.type === 'password') {
                passwordInput.type = 'text';
                toggleButton.innerHTML = '<i class="fas fa-eye-slash"></i>';
            } else {
                passwordInput.type = 'password';
                toggleButton.innerHTML = '<i class="fas fa-eye"></i>';
            }
        });
    }
    
    // Email validation
    const emailInput = document.getElementById('email');
    if (emailInput) {
        emailInput.addEventListener('blur', function() {
            const email = emailInput.value;
            const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
            
            if (email && !emailRegex.test(email)) {
                emailInput.classList.add('is-invalid');
                if (!emailInput.nextElementSibling || !emailInput.nextElementSibling.classList.contains('invalid-feedback')) {
                    const feedback = document.createElement('div');
                    feedback.className = 'invalid-feedback';
                    feedback.textContent = 'Please enter a valid email address.';
                    emailInput.parentElement.appendChild(feedback);
                }
            } else {
                emailInput.classList.remove('is-invalid');
                const feedback = emailInput.parentElement.querySelector('.invalid-feedback');
                if (feedback) {
                    feedback.remove();
                }
            }
        });
    }
    
    // Consent checkbox validation
    const consentCheckbox = document.getElementById('consent');
    const submitButton = document.querySelector('button[type="submit"]');
    
    if (consentCheckbox && submitButton) {
        consentCheckbox.addEventListener('change', function() {
            if (consentCheckbox.checked) {
                submitButton.disabled = false;
                submitButton.classList.remove('btn-secondary');
                submitButton.classList.add('btn-primary');
            } else {
                submitButton.disabled = true;
                submitButton.classList.remove('btn-primary');
                submitButton.classList.add('btn-secondary');
            }
        });
        
        // Initialize button state
        if (!consentCheckbox.checked) {
            submitButton.disabled = true;
            submitButton.classList.remove('btn-primary');
            submitButton.classList.add('btn-secondary');
        }
    }
    
    // Loading state for forms
    const downloadForm = document.getElementById('downloadForm');
    if (downloadForm) {
        downloadForm.addEventListener('submit', function() {
            const submitBtn = downloadForm.querySelector('button[type="submit"]');
            submitBtn.disabled = true;
            submitBtn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Processing...';
        });
    }
    
    // Tooltip initialization
    const tooltips = document.querySelectorAll('[data-bs-toggle="tooltip"]');
    tooltips.forEach(tooltip => {
        new bootstrap.Tooltip(tooltip);
    });
    
    // Copy to clipboard functionality
    const copyButtons = document.querySelectorAll('.copy-btn');
    copyButtons.forEach(button => {
        button.addEventListener('click', function() {
            const textToCopy = button.getAttribute('data-copy');
            navigator.clipboard.writeText(textToCopy).then(() => {
                const originalText = button.innerHTML;
                button.innerHTML = '<i class="fas fa-check"></i> Copied!';
                button.classList.remove('btn-outline-secondary');
                button.classList.add('btn-success');
                
                setTimeout(() => {
                    button.innerHTML = originalText;
                    button.classList.remove('btn-success');
                    button.classList.add('btn-outline-secondary');
                }, 2000);
            });
        });
    });
});

// Utility function to format file sizes
function formatFileSize(bytes) {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Utility function to format numbers with commas
function formatNumber(num) {
    return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

// Session timeout warning
let sessionTimeout;
let warningTimeout;

function resetSessionTimer() {
    clearTimeout(sessionTimeout);
    clearTimeout(warningTimeout);
    
    // Show warning after 50 minutes
    warningTimeout = setTimeout(() => {
        if (confirm('Your session will expire in 10 minutes. Do you want to continue?')) {
            // Make a request to refresh the session
            fetch('/api/tables', { method: 'HEAD' });
            resetSessionTimer();
        }
    }, 50 * 60 * 1000);
    
    // Auto logout after 60 minutes
    sessionTimeout = setTimeout(() => {
        alert('Your session has expired. You will be redirected to the login page.');
        window.location.href = '/logout';
    }, 60 * 60 * 1000);
}

// Start session timer if user is authenticated
if (document.querySelector('.navbar-nav a[href*="logout"]')) {
    resetSessionTimer();
    
    // Reset timer on user activity
    document.addEventListener('click', resetSessionTimer);
    document.addEventListener('keypress', resetSessionTimer);
}