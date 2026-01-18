import hashlib
import secrets


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain password against a hashed password
    """
    try:
        # Split the stored hash to get salt and hash
        salt, stored_hash = hashed_password.split('$')
        # Hash the provided password with the stored salt
        pwdhash = hashlib.pbkdf2_hmac('sha256',
                                      plain_password.encode('utf-8'),
                                      salt.encode('ascii'),
                                      100000)
        # Encode the new hash to compare with the stored hash
        pwdhash = pwdhash.hex()
        return secrets.compare_digest(pwdhash, stored_hash)
    except Exception:
        return False


def get_password_hash(password: str) -> str:
    """
    Generate a hash for the provided password using pbkdf2
    """
    # Generate a random salt
    salt = secrets.token_hex(32)
    # Hash the password with the salt
    pwdhash = hashlib.pbkdf2_hmac('sha256',
                                  password.encode('utf-8'),
                                  salt.encode('ascii'),
                                  100000)
    # Store salt and hash together
    pwdhash = pwdhash.hex()
    return f"{salt}${pwdhash}"