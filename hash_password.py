import hashlib

def hash_password_md5(password: str) -> str:
    """Encrypts a plain-text password using MD5 and returns the 32-character lowercase hash."""
    return hashlib.md5(password.encode('utf-8')).hexdigest()

print(hash_password_md5("Kavtaradze19"))