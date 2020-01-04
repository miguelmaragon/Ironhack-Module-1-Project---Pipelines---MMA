import random
import imgkit
import pandas
import os


def css_f():
    return str("""
    <style type=\"text/css\">
    table {
    color: #333;
    font-family: Helvetica, Arial, sans-serif;
    width: 100%;
    background-color: #FFFFFF;
    border-color: #7ea8f8;
    border-collapse: collapse;
    border-style: solid;
    border-spacing: 0;
    color: #000000;
    }
    td, th {
    border-color: #7ea8f8;
    height: 30px;
    text-align: center;
    }
    th {
    background: #DFDFDF;
    font-weight: bold;
    }
    td {
    background: #FFFFFF;
    text-align: center;
    }
    table tr:nth-child(odd) td{
    background-color: white;
    }
    </style>
    """)


def dataframe_to_image(data, css, outputfile="out.png", format="png"):
    fn = str(random.random() * 100000000).split(".")[0] + ".html"
    try:
        os.remove(fn)
    except:
        None
    text_file = open(fn, "a")

    # write the CSS
    text_file.write(css)
    # write the HTML-ized Pandas DataFrame
    text_file.write(data.to_html())
    text_file.close()

    # See IMGKit options for full configuration,
    # e.g. cropping of final image
    imgkitoptions = {"format": format}

    imgkit.from_file(fn, outputfile, options=imgkitoptions)
    os.remove(fn)
