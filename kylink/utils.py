import json
import os
import base64
from io import BytesIO
import matplotlib.pyplot as plt


# Patch
def ensure_matplotlib_patch():
    # _old_show = plt.show
  
    def show():
        buf = BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        # Encode to a base64 str
        img = 'data:image/png;base64,' + \
        base64.b64encode(buf.read()).decode('utf-8')
        # Write to stdout
        print(img)
        plt.clf()

    plt.show = show

os.environ['MPLBACKEND'] = 'AGG'

ensure_matplotlib_patch()

class Utils:
    def __init__(self):
        pass

    def install():
        ensure_matplotlib_patch()

    def table(list_of_elements):
        print("data:table/" + json.dumps(list_of_elements))

