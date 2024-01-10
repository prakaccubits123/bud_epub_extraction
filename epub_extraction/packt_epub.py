from bs4 import BeautifulSoup
import shutil
import os
import ebooklib
from ebooklib import epub
from PIL import Image
from pix2tex.cli import LatexOCR
from bs4 import BeautifulSoup, NavigableString
from extract_epub_table import process_book_page
from utils import (
    timeit,
    mongo_init,
    parse_table,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    generate_unique_id,
    get_s3,
    latext_to_text_to_speech,
)


latex_ocr = LatexOCR()

# change folder and bucket name as required.
bucket_name = "bud-datalake"
# folder_name = "Books/Oct29-1/"
folder_name = "Books/Oct29-1/"
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_testing")
db2 = mongo_init("epub_packt")
oct_toc = db2.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db2.oct_chapters
files_with_error = db.files_with_error
extracted_books = db.extracted_books
publisher_collection = db.publishers


def download_aws_image(key, book):
    try:
        if os.path.exists(book):
            shutil.rmtree(book)
        os.makedirs(book)
        local_path = os.path.join(book, os.path.basename(key))
        s3 = get_s3()
        s3.download_file(bucket_name, key, local_path)
        return os.path.abspath(local_path)
    except Exception as e:
        print(e)


@timeit
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
                # parent2 = child.find_parent("figure", class_="mediaobject")

                if parent:
                    print("hellowww")
                    figcaption = parent.find("figcaption")
                    if figcaption:
                        img["caption"] = figcaption.get_text(strip=True)
                        print("this is image_caption", img["caption"])
                    else:
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            next_sib_class = next_sibling.get("class", [""])[0]
                            print(next_sib_class)
                            if next_sib_class == "packt_figref":
                                img["caption"] = next_sibling.get_text(strip=True)
                                print("this is image_caption", img["caption"])

                # elif parent2:
                #     print("hello")
                #     next_sibling = parent2.find_next_sibling()
                #     if next_sibling:
                #         next_sib_class = next_sibling.get("class", [""])[0]
                #         print(next_sib_class)
                #         if next_sib_class == "packt_figref":
                #             img["caption"] = next_sibling.get_text(strip=True)
                #             print("this is image_caption", img["caption"])

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
                # parent2 = child.find_parent("figure", class_="mediaobject")

                if parent:
                    tablecap = parent.find("figcaption")
                    if tablecap:
                        caption_text = tablecap.get_text(strip=True)
                        print("this is table caption", caption_text)
                    else:
                        next_sibling = parent.find_next_sibling()
                        if next_sibling:
                            next_sib_class = next_sibling.get("class", [""])[0]
                            if next_sib_class == "packt_figref":
                                caption_text = next_sibling.get_text(strip=True)
                                print("this is table caption", caption_text)
                else:
                    next_sibling = child.find_next_sibling()
                    if next_sibling:
                        next_sib_class = next_sibling.get("class", [""])[0]
                        if next_sib_class == "packt_figref":
                            caption_text = next_sibling.get_text(strip=True)
                            print("this is table caption", caption_text)

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

            elif (child.name == "div" and "math-display" in child.get("class", [])) or (
                child.name == "p" and "center" in child.get("class", [])
            ):
                print("equaiton here")
                equation_image = child.find("img")
                equation_Id = generate_unique_id()
                if equation_image:
                    aws_path = f"{s3_base_url}/{folder_name}{book}/OEBPS/"
                    img_url = aws_path + equation_image["src"]
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
                    print("this is equation image from equation class")
                    # os.remove(equation_image_path)
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
                    print(filename)
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


# taking books from publishers collection and checking if it has pattern (figure tag inside any html file)


def find_figure_tag_in_html(html_content):
    soup = BeautifulSoup(html_content, "html.parser")
    div_with_figure_class = soup.find_all("figure")
    return div_with_figure_class


def get_html_from_epub(epub_path):
    book = epub.read_epub(epub_path)
    # Iterate through items in the EPUB book
    for item in book.get_items():
        # Check if the item is of type 'text'
        if item.get_type() == ebooklib.ITEM_DOCUMENT:
            # Extract the HTML content
            html_content = item.get_content().decode("utf-8", "ignore")

            # Find figure tags in the HTML content
            figure_tags = find_figure_tag_in_html(html_content)

            # If figure tags are found, return the first one and break the loop
            if figure_tags:
                return figure_tags[0]
    # Return None if no figure tags are found
    return None


# taking books from publishers collection and checking if it has pattern (figure tag inside any html file)
# figure_pattern = []
# packt_remaining = []
# extracted = []
# for book in publisher_collection.find():
#     if (
#         "publishers" in book
#         and book["publishers"]
#         and book["publishers"][0].startswith("Packt")
#     ):
#         if "s3_key" in book:
#             s3_key = book["s3_key"]
#             bookname = book["s3_key"].split("/")[-2]
#             already_extracted = extracted_books.find_one({"book": bookname})
#             if not already_extracted:
#                 print("e")
#                 epub_path = download_epub_from_s3(bookname, s3_key)
#                 if not epub_path:
#                     continue
#                 figure_tag = get_html_from_epub(epub_path)
#                 if figure_tag:
#                     if os.path.exists(epub_path):
#                         os.remove(epub_path)
#                     print("figure tag found")
#                     figure_pattern.append(s3_key)
#                     with open("packt_figure_pattern.txt", "a") as file:
#                         file.write(f"{s3_key}\n")
#                     # get_book_data(bookname)
#                 else:
#                     print("no figure tag")
#                     packt_remaining.append(s3_key)
#                     if os.path.exists(epub_path):
#                         os.remove(epub_path)
#             else:
#                 print(f"this {bookname}already extracted")
#                 extracted.append(bookname)

# print("total figure", len(figure_pattern))
# print("total without figure", len(packt_remaining))

# f = open("packt_figure_pattern.txt", "w")
# f.write(str(figure_pattern))

# f = open("packt_remaining_pattern.txt", "w")
# f.write(str(packt_remaining))

books = [
    "Books/Oct29-1/Microsoft Power Platform Enterprise Architecture - Second Edition (9781804612637)/9781804612637.epub",
    "Books/Oct29-1/Generative AI with LangChain (9781835083468)/9781835083468.epub",
    "Books/Oct29-1/AWS for Solutions Architects - Second Edition (9781803238951)/9781803238951.epub",
    "Books/Oct29-1/Incident Response in the Age of Cloud (9781800569218)/9781800569218.epub",
    "Books/Oct29-1/Generative AI with Python and TensorFlow 2 (9781800200883)/9781800200883.epub",
    "Books/Oct29-1/Flutter Cookbook - Second Edition (9781803245430)/9781803245430.epub",
    "Books/Oct29-1/C_ 10 and .NET 6 – Modern Cross-Platform Development - Sixth Edition (9781801077361)/9781801077361.epub",
    "Books/Oct29-1/Full Stack Development with Spring Boot 3 and React - Fourth Edition (9781805122463)/9781805122463.epub",
    "Books/Oct29-1/Python Machine Learning - Third Edition (9781789955750)/9781789955750.epub",
    "Books/Oct29-1/Python Real-World Projects (9781803246765)/9781803246765.epub",
    "Books/Oct29-1/Mastering Blockchain - Fourth Edition (9781803241067)/9781803241067.epub",
    "Books/Oct29-1/Microservices with Spring Boot 3 and Spring Cloud - Third Edition (9781805128694)/9781805128694.epub",
    "Books/Oct29-1/iOS 17 Programming for Beginners - Eighth Edition (9781837630561)/9781837630561.epub",
    "Books/Oct29-1/Terraform Cookbook - Second Edition (9781804616420)/9781804616420.epub",
    "Books/Oct29-1/Solutions Architect_s Handbook - Second Edition (9781801816618)/9781801816618.epub",
    "Books/Oct29-1/Mastering Linux Security and Hardening - Third Edition (9781837630516)/9781837630516.epub",
    "Books/Oct29-1/Docker Deep Dive - Second Edition (9781835081709)/9781835081709.epub",
    "Books/Oct29-1/Mastering Kubernetes - Fourth Edition (9781804611395)/9781804611395.epub",
    "Books/Oct29-1/Extreme C (9781789343625)/9781789343625.epub",
    "Books/Oct29-1/Learning C_ by Developing Games with Unity - Seventh Edition (9781837636877)/9781837636877.epub",
    "Books/Oct29-1/Learning OpenCV 5 Computer Vision with Python - Fourth Edition (9781803230221)/9781803230221.epub",
    "Books/Oct29-1/Functional Python Programming - Third Edition (9781803232577)/9781803232577.epub",
    "Books/Oct29-1/Responsive Web Design with HTML5 and CSS - Fourth Edition (9781803242712)/9781803242712.epub",
    "Books/Oct29-1/Microservices with Spring Boot and Spring Cloud - Second Edition (9781801072977)/9781801072977.epub",
    "Books/Oct29-1/9781803237671/9781803237671.epub",
    "Books/Oct29-1/9781801074308/9781801074308.epub",
    "Books/Oct29-1/9781800568105/9781800568105.epub",
    "Books/Oct29-1/The Music Producer_s Ultimate Guide to FL Studio 21 - Second Edition (9781837631650)/9781837631650.epub",
    "Books/Oct29-1/Building Analytics Teams (9781800203167)/9781800203167.epub",
    "Books/Oct29-1/9781803239118/9781803239118.epub",
    "Books/Oct29-1/The Kaggle Book (9781801817479)/9781801817479.epub",
    "Books/Oct29-1/Blockchain with Hyperledger Fabric - Second Edition (9781839218750)/9781839218750.epub",
    "Books/Oct29-1/Modern C__ Programming Cookbook - Second Edition (9781800208988)/9781800208988.epub",
    "Books/Oct29-1/Azure Strategy and Implementation Guide - Third Edition (9781838986681)/9781838986681.epub",
    "Books/Oct29-1/Machine Learning with PyTorch and Scikit-Learn (9781801819312)/9781801819312.epub",
    "Books/Oct29-1/Transformers for Natural Language Processing (9781800565791)/9781800565791.epub",
    "Books/Oct29-1/An Atypical ASP.NET Core 6 Design Patterns Guide - Second Edition (9781803249841)/9781803249841.epub",
    "Books/Oct29-1/The Kaggle Workbook (9781804611210)/9781804611210.epub",
    "Books/Oct29-1/Unity 3D Game Development (9781801076142)/9781801076142.epub",
    "Books/Oct29-1/Microsoft Office 365 and SharePoint Online Cookbook - Second Edition (9781803243177)/9781803243177.epub",
    "Books/Oct29-1/SAP on Azure Implementation Guide (9781838983987)/9781838983987.epub",
]
for num, book in enumerate(books):
    bookname = book.split("/")[-2]
    print(num)
    already_extracted = extracted_books.find_one({"book": bookname})
    if not already_extracted:
        get_book_data(bookname)
        print("E")

# get_book_data("AWS for Solutions Architects - Second Edition (9781803238951)")
