from fpdf import FPDF


def to_pdf(list_reports):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_xy(0, 0)
    pdf.set_font('arial', 'B', 12)
    pdf.cell(60)
    pdf.cell(80, 25, "A Tabular and Graphical Report of Forbes Billionaires 2018", 0, 2, 'C')
    pdf.cell(90, 10, " ", 0, 2, 'C')
    pdf.cell(-40)
    for x in list_reports:
        pdf.image('./reports/' + x + '.png', x=None, y=None, w=175, h=0, type='', link='')
        pdf.cell(90, 10, " ", 0, 2, 'C')
    pdf.output('report_all.pdf', 'F')
