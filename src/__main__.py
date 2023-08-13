# 1. Install this package
import micropip
await micropip.install('pyodide-http')

# 2. Patch requests
import pyodide_http
pyodide_http.patch_all()  # Patch all libraries

