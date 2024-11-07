import fitz
from pathlib import Path
from kbs.common.rw import save_in_minio
from kbs.common.tools import extract_tags


def extract_pdf_data(minio_client,pdf_file, minio_path):

    if not pdf_file.endswith(".pdf"):
        raise ValueError("path must be a pdf file")

    filepath = Path(pdf_file)
    try:
        doc = fitz.open(pdf_file, filetype="pdf")
    except fitz.fitz.FileDataError:
        return {}
    pages = []
    tags = []
    images = []
    for page_num, page in enumerate(doc, start=1):  # iterate the document pages
        page_data = {"id": page_num, "fulltext": "", "tables": []}
        text_content = (
            page.get_text().encode("utf8").decode("utf-8")
        )  # get plain text (is in UTF-8)
        page_data.update({"fulltext": text_content})

        tags.extend(extract_tags(page_data.get("fulltext")))

        images_list = page.get_images(full=True)
        for image_index, img in enumerate(
            images_list, start=1
        ):  # enumerate the image list
            xref = img[0]

            pix = fitz.Pixmap(doc, xref)  # create a Pixmap

            if pix.n - pix.alpha > 3:  # CMYK: convert to RGB first
                pix = fitz.Pixmap(fitz.csRGB, pix)

            location = (
                f"tmp/{filepath.parts[-1]}_page_{page_num}-image_{image_index}.png"
            )
            location = Path(location)
            try:
                pix.save(location)
                images.append(save_in_minio(minio_client,location))  # save the image as png
                location.unlink()
                pix = None

            except RuntimeError:
                pass
           
        current_tables = page.find_tables()

        for tab in current_tables:
            try:
                df = tab.to_pandas()
                tab_json_str = df.to_json()
                page_data["tables"].append(tab_json_str)
            except IndexError:
                pass
            

        pages.append(page_data)

    output_dict = {
        "name": filepath.parts[-1],
        "path": str(minio_path),
        # "type": "TYPE",
        # "rubrique": "RU",
        "extension": filepath.name.split(".")[-1],
        "tags": list(set(tags)),
        "images": images,
        "pages": pages,
    }
    return output_dict