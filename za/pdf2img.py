import PyPDF2
import io
from wand.image import Image


def pdf_page_to_png(src_pdf, pagenum = 0, resolution = 72):
    """
    Returns specified PDF page as wand.image.Image png.
    :param PyPDF2.PdfFileReader src_pdf: PDF from which to take pages.
    :param int pagenum: Page number to take.
    :param int resolution: Resolution for resulting png in DPI.
    """
    dst_pdf = PyPDF2.PdfFileWriter()
    dst_pdf.addPage(src_pdf.getPage(pagenum))

    pdf_bytes = io.BytesIO()
    dst_pdf.write(pdf_bytes)
    pdf_bytes.seek(0)

    img = Image(file = pdf_bytes, resolution = resolution)
    img.convert("png")

    img.save('/Users/summer/tmp/bird.png')

pdf_page_to_png('/Users/summer/tmp/bird.pdf',5,300)