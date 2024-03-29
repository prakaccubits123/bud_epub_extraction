import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup
import os
from PIL import Image
from pix2tex.cli import LatexOCR
from bs4 import BeautifulSoup, NavigableString
from utils import (
    timeit,
    mongo_init,
    parse_table,
    get_all_books_names,
    get_s3,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    generate_unique_id,
    latext_to_text_to_speech,
)

latex_ocr = LatexOCR()

# change folder and bucket name as required.
bucket_name = "bud-datalake"
# folder_name = "Books/Oct29-1/"
folder_name = "Books/Oct29-Wiley/"
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_testing")
oct_toc = db.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db.oct_chapters
files_with_error = db.files_with_error
extracted_books = db.extracted_books
publisher_collection = db.publishers


def download_aws_image(key, book):
    try:
        book_folder = os.path.join(folder_name, book)
        os.makedirs(book_folder, exist_ok=True)
        local_path = os.path.join(book_folder, os.path.basename(key))
        s3 = get_s3()
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)
        return None


def download_epub_from_s3(bookname, s3_key):
    try:
        local_path = os.path.abspath(os.path.join(folder_name, f"{bookname}.epub"))
        os.makedirs(folder_name, exist_ok=True)
        s3 = get_s3()
        s3.download_file(bucket_name, s3_key, local_path)
        return local_path
    except Exception as e:
        print(e)
        return None


@timeit
def parse_html_to_json(html_content, book, filename):
    # html_content = get_file_object_aws(book, filename)
    soup = BeautifulSoup(html_content, "html.parser")
    # h_tag = get_heading_tags(soup, h_tag=[])
    section_data = extract_data(soup.find("body"), book, filename, section_data=[])
    return section_data


def extract_data(elem, book, filename, section_data=[]):
    for child in elem.children:
        temp = {}
        if isinstance(child, NavigableString):
            if child.strip():
                if section_data:
                    section_data[-1]["content"] += child + " "
                else:
                    temp["title"] = ""
                    temp["content"] = child + " "
                    temp["figures"] = []
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

        elif child.name:
            if child.name in ["h1", "h2", "h3", "h4", "h5", "h6"]:
                parent_figure = child.find_parent("figure")
                if not parent_figure:
                    if section_data and section_data[-1]["content"].endswith(
                        "{{title}} "
                    ):
                        section_data[-1]["content"] += child.text.strip() + " "
                    else:
                        temp["title"] = child.text.strip()
                        temp["content"] = "{{title}}" + " "
                        temp["figures"] = []
                        temp["tables"] = []
                        temp["code_snippet"] = []
                        temp["equations"] = []

            elif child.name == "img":
                print("figure here from img")
                img = {}
                img["id"] = generate_unique_id()
                aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                img["url"] = aws_path + child["src"]

                parent = child.find_parent("figure")
                if parent:
                    figcaption = parent.find("figcaption")
                    if figcaption:
                        figcap = figcaption.find("p")
                        if figcap:
                            img["caption"] = figcap.get_text("")
                            print(img["caption"])
                if section_data:
                    section_data[-1]["content"] += "{{figure:" + img["id"] + "}} "
                    if "figures" in section_data[-1]:
                        section_data[-1]["figures"].append(img)
                    else:
                        section_data[-1]["figures"] = [img]

                else:
                    temp["title"] = ""
                    temp["content"] = "{{figure:" + img["id"] + "}} "
                    temp["figures"] = [img]
                    temp["tables"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "table":
                print("table here")
                caption_text = ""
                parent = child.find_parent("figure")
                if parent:
                    tabcaption = parent.find("figcaption")
                    if tabcaption:
                        tabcap = tabcaption.find("p")
                        if tabcap:
                            caption_text = tabcap.get_text(strip=True)
                            print(caption_text)
                table_id = generate_unique_id()
                table_data = parse_table(child)
                table = {"id": table_id, "data": table_data, "caption": caption_text}
                if section_data:
                    section_data[-1]["content"] += "{{table:" + table["id"] + "}} "
                    if "tables" in section_data[-1]:
                        section_data[-1]["tables"].append(table)
                        # section_data[-1]['tables'] = [table]
                    else:
                        section_data[-1]["tables"] = [table]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{table:" + table["id"] + "}} "
                    temp["tables"] = [table]
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = []

            elif child.name == "div" and (
                "equationNumbered" in child.get("class", [])
                or "informalEquation" in child.get("class", [])
            ):
                equation_image = child.find("img")
                equation_Id = generate_unique_id()
                if equation_image:
                    aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                    img_url = aws_path + equation_image["src"]
                    print("This is equation image")
                    img_key = img_url.replace(s3_base_url + "/", "")
                    equation_image_path = download_aws_image(img_key, book)
                    if not equation_image_path:
                        continue
                    try:
                        img = Image.open(equation_image_path)
                    except Exception as e:
                        print("from image equation", e)
                        continue
                    try:
                        latex_text = latex_ocr(img)
                    except Exception as e:
                        print("error while extracting latex code from image", e)
                        continue
                    text_to_speech = latext_to_text_to_speech(latex_text)
                    eqaution_data = {
                        "id": equation_Id,
                        "text": latex_text,
                        "text_to_speech": text_to_speech,
                    }
                    print(equation_image_path)
                    print("this is equation image")
                    os.remove(equation_image_path)
                else:
                    continue
                if section_data:
                    section_data[-1]["content"] += "{{equation:" + equation_Id + "}} "
                    if "equations" in section_data[-1]:
                        section_data[-1]["equations"].append(eqaution_data)
                    else:
                        section_data[-1]["equations"] = [eqaution_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{equation:" + equation_Id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = []
                    temp["equations"] = [eqaution_data]

            # code oreilly publication
            elif child.name == "pre":
                print("code here")
                code_tags = child.find_all("code")
                code = ""
                if code_tags:
                    code = " ".join(
                        code_tag.get_text(strip=True) for code_tag in code_tags
                    )
                else:
                    code = child.get_text(strip=True)
                code_id = generate_unique_id()
                code_data = {"id": code_id, "code_snippet": code}
                if section_data:
                    section_data[-1]["content"] += "{{code_snippet:" + code_id + "}} "
                    if "code_snippet" in section_data[-1]:
                        section_data[-1]["code_snippet"].append(code_data)

                    else:
                        section_data[-1]["code_snippet"] = [code_data]
                else:
                    temp["title"] = ""
                    temp["content"] = "{{code_snippet:" + code_id + "}} "
                    temp["tables"] = []
                    temp["figures"] = []
                    temp["code_snippet"] = [code_data]
                    temp["equations"] = []
            elif child.contents:
                section_data = extract_data(
                    child, book, filename, section_data=section_data
                )
        if temp:
            section_data.append(temp)
    return section_data


@timeit
def get_book_data(book):
    print("Book Name >>> ", book)
    toc = []
    # check if book exists in db toc collection
    db_toc = oct_toc.find_one({"book": book})
    if db_toc:
        toc = db_toc["toc"]
    if not toc:
        error = ""
        try:
            # get table of content
            toc_content = get_file_object_aws(book, "toc.ncx", folder_name, bucket_name)
            if toc_content:
                toc = get_toc_from_ncx(toc_content)
            else:
                toc_content = get_file_object_aws(
                    book, "toc.xhtml", folder_name, bucket_name
                )
                if toc_content:
                    toc = get_toc_from_xhtml(toc_content)
        except Exception as e:
            error = str(e)
            print(f"Error while parsing {book} toc >> {e}")
        if not toc:
            oct_no_toc.insert_one({"book": book, "error": error})
        else:
            oct_toc.insert_one({"book": book, "toc": toc})

    files = []
    order_counter = 0
    prev_filename = None

    for label, content in toc:
        content_split = content.split("#")
        if len(content_split) > 0:
            filename = content_split[0]
            if filename != prev_filename:
                if filename not in files:
                    file_in_error = files_with_error.find_one(
                        {"book": book, "filename": filename}
                    )
                    if file_in_error:
                        files_with_error.delete_one(
                            {"book": book, "filename": filename}
                        )
                    chapter_in_db = oct_chapters.find_one(
                        {"book": book, "filename": filename}
                    )
                    if chapter_in_db:
                        if chapter_in_db["sections"]:
                            continue
                        elif not chapter_in_db["sections"]:
                            oct_chapters.delete_one(
                                {"book": book, "filename": filename}
                            )

                    html_content = get_file_object_aws(
                        book, filename, folder_name, bucket_name
                    )

                    if html_content:
                        try:
                            json_data = parse_html_to_json(html_content, book, filename)
                            oct_chapters.insert_one(
                                {
                                    "book": book,
                                    "filename": filename,
                                    "sections": json_data,
                                    "order": order_counter,
                                }
                            )
                            order_counter += 1
                        except Exception as e:
                            print(f"Error while parsing {filename} html >> {e}")
                            files_with_error.insert_one(
                                {"book": book, "filename": filename, "error": e}
                            )
                            # clear mongo
                            oct_chapters.delete_many({"book": book})
                    else:
                        print("no html content found : ", filename)
                        files_with_error.insert_one(
                            {
                                "book": book,
                                "filename": filename,
                                "error": "no html content found",
                            }
                        )
                    files.append(filename)
                prev_filename = filename

    book_data = {"book": book, "extraction": "completed"}
    extracted_books.insert_one(book_data)


# def find_figure_tag_in_html(html_content):
#     soup = BeautifulSoup(html_content, "html.parser")
#     figure_tags = soup.find_all("figure")
#     return figure_tags


# def get_html_from_epub(epub_path):
#     book = epub.read_epub(epub_path)
#     # Iterate through items in the EPUB book
#     for item in book.get_items():
#         # Check if the item is of type 'text'
#         if item.get_type() == ebooklib.ITEM_DOCUMENT:
#             # Extract the HTML content
#             html_content = item.get_content().decode("utf-8", "ignore")

#             # Find figure tags in the HTML content
#             figure_tags = find_figure_tag_in_html(html_content)

#             # If figure tags are found, return the first one and break the loop
#             if figure_tags:
#                 return figure_tags[0]
#     # Return None if no figure tags are found
#     return None



def find_figure_tag_in_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    figure_tags = soup.find_all('div', class_="figure")
    return figure_tags

def find_image_paragraph_in_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    image_paragraphs = soup.find_all('div', class_='center')
    return image_paragraphs

def get_html_from_epub(epub_path):
    book = epub.read_epub(epub_path)
    figure_found = False

    for item in book.get_items():
        if item.get_type() == ebooklib.ITEM_DOCUMENT:
            html_content = item.get_content().decode("utf-8", "ignore")
            figure_tags = find_figure_tag_in_html(html_content)

            if figure_tags:
                figure_found = True
                return "div_wrap"

    # If figure tag is not found in any HTML file, check for p element with class name "image"
    if not figure_found:
        for item in book.get_items():
            if item.get_type() == ebooklib.ITEM_DOCUMENT:
                html_content = item.get_content().decode("utf-8", "ignore")
                image_paragraphs = find_image_paragraph_in_html(html_content)

                if image_paragraphs:
                    return "p_center"

    # Return None if neither figure tags nor p element with class name "image" is found
    return None

# taking books from publishers collection and checking if it has pattern (figure tag inside any html file)
extracted = []
not_extracted = []
# for book in publisher_collection.find():
#     if (
#         "publishers" in book
#         and book["publishers"]
#         and book["publishers"][0].startswith("Wiley")
#     ):
#         if "s3_key" in book:
#             s3_key = book["s3_key"]
#             bookname = book["s3_key"].split("/")[-2]
#             already_extracted = extracted_books.find_one({"book": bookname})
#             if not already_extracted:
#                not_extracted.append(bookname)
#             else:
#                 extracted.append(book)
#                 print(f"this {book['title']} already extracted")

books = get_all_books_names(bucket_name, folder_name)
print(len(books))
for book in books:
    already_extracted = extracted_books.find_one({"book": book})
    if not already_extracted:
        not_extracted.append(book)
    else:
        extracted.append(book)
        print(f"this {book} already extracted")

print("total extracted", len(extracted))
print("total not extracted", len(not_extracted))
f = open("wiley2_not_extracted.txt", "w")
f.write(str(not_extracted))



# get all books from aws and checking if it has pattern (figure tag inside any html file)
# extracted = []
# books = get_all_books_names(bucket_name, folder_name)
# print(len(books))
# wiley_pcentern_tags = []
# # wiley_without_pcenter_tags = []/
# for book in books:
#     already_extracted = extracted_books.find_one({"book": book})
#     s3_key = f"{folder_name}{book}/{book}.epub"
#     print(s3_key)
#     if not already_extracted:
#         print("e")
#         epub_path = download_epub_from_s3(book, s3_key)
#         if not epub_path:
#             continue
#         try:
#             figure_tag = get_html_from_epub(epub_path)
#         except Exception as e:
#             print("error while identify figure tag", e)
#             continue
#         if figure_tag:
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#             # print("figure tag found")
#             # book_with_figure_tags.append(book)
#             # get_book_data(book)
#         else:
#             print("no pattern")
#             wiley_pcentern_tags.append(book)
#             if os.path.exists(epub_path):
#                 os.remove(epub_path)
#     else:
#         print('already_extracted')
#         extracted.append(book)


# print("total books", len(books))
# print("total extracted", len(extracted))
# print("total books without p center and image wrap tag", len(wiley_pcentern_tags))
# f = open("wiley_aws_books_without_pce_div_wrap", "w")
# f.write(str(wiley_pcentern_tags))
# print("total books with out figure tag", len(book_with_figure_tags))
# f = open("wiley_aws_books_without_figure", "w")
# f.write(str(books_with_out_figure_tags))

# get_html_from_epub("/home/bud-data-extraction/datapipeline/Books/Oct29-Wiley/9780470317235.epub")
