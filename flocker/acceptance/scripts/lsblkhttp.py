"""
HTTP server that returns the size of the mounted volume.
"""

from subprocess import check_output

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(s):
        s.send_response(200)
        s.end_headers()
        if "is_http" in s.path:
            s.wfile.write(b"hi")
        else:
            try:
                mounts = {}
                for line in check_output(['mount', '-l']).split('\n'):
                    parts = line.split(' ')
                    if len(parts) > 2:
                        mounts[parts[2]] = parts[0]
                device_path = mounts["/data"]

                command = [b"/bin/lsblk", b"--noheadings", b"--bytes",
                           b"--output", b"SIZE", device_path.encode("ascii")]
                command_output = check_output(command).split(b'\n')[0]
                device_size = str(int(command_output.strip().decode("ascii")))
            except Exception as e:
                s.wfile.write(str(e.__class__) + ": " + str(e))
            else:
                s.wfile.write(device_size)
        s.wfile.close()

httpd = HTTPServer((b"0.0.0.0", 8080), Handler)
httpd.serve_forever()
