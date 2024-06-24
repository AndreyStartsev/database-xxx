from PIL import Image

try:
    im = Image.open("./images/logo.jpg")
except:
    im = "🤖"

LOGO = im
PAGE_CONFIG = dict(page_title="xxx-demo",
                   page_icon=LOGO,
                   layout="wide",
                   menu_items={
                       'Get Help': 'https://chat.openai.com/chat',
                       'About': "### BBB"
                   })
STYLES = {'navtab': {'background-color': '#111',
                     'color': '#d3d3d3',
                     'font-size': '18px',
                     'transition': '.3s',
                     'white-space': 'nowrap',
                     'text-transform': 'uppercase'},
          'tabOptionsStyle': {':hover :hover': {'color': 'green',
                                                'cursor': 'pointer'}},
          'iconStyle': {'position': 'fixed',
                        'left': '7.5px',
                        'text-align': 'left'},
          'tabStyle': {'list-style-type': 'none',
                       'margin-bottom': '30px',
                       'padding-left': '30px'}}
