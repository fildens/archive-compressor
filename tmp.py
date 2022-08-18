import ssl
from pathlib import Path

print(ssl.OPENSSL_VERSION)


Path(r'D:\test.txt').unlink(missing_ok=True)