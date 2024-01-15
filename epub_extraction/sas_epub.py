from bs4 import BeautifulSoup, NavigableString
import os
import shutil
from pix2tex.cli import LatexOCR
from PIL import Image
from utils import (
    timeit,
    mongo_init,
    parse_table,
    generate_unique_id,
    get_file_object_aws,
    get_toc_from_ncx,
    get_toc_from_xhtml,
    get_s3,
    latext_to_text_to_speech
)

latex_ocr=LatexOCR()

bucket_name = "bud-datalake"
folder_name = "Books/Oct29-1/"
s3_base_url = "https://bud-datalake.s3.ap-southeast-1.amazonaws.com"


db = mongo_init("epub_testing")
db2 = mongo_init("epub_sas")
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
        return None
    
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
                img['caption']=''
                parent = child.find_parent("div", class_="figure")
                parent2 = child.find_parent("p", class_=["FIG","Gearheart_P-Styles_AFigure_Center"])
                if parent:
                    print("yes..")
                    figcap=parent.find('p', class_="title")
                    if figcap:
                        img['caption']=figcap.get_text(strip=True)
                        print("this is image caption", img['caption'])
                elif parent2:
                    previous_sibling= parent2.find_previous_sibling()
                    if previous_sibling:
                        prev_sib_class= previous_sibling.get('class',[''])[0]
                        if prev_sib_class=="FC" or prev_sib_class=="Gearheart_P-Styles_ACaptionHead":
                            img['caption'] = previous_sibling.get_text(strip=True)
                            print("this is image caption", img['caption'])

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
                tab_parent = child.find_parent("div", class_="table-contents")
                if tab_parent:
                    tab_parent2 = tab_parent.find_parent("div", class_="table")
                    if tab_parent2:
                        tab_cap = tab_parent2.find("p", class_="title")
                        if tab_cap:
                            caption_text = tab_cap.get_text(strip=True)
                            print("this is table caption", caption_text)
                else:
                    previous_sibling= child.find_previous_sibling()
                    if previous_sibling:
                        prev_sib_class=previous_sibling.get('class',[''])[0]
                        if prev_sib_class=='Gearheart_P-Styles_ACaptionHead':
                            caption_text=previous_sibling.get_text(strip=True)
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


            
            elif child.name == "p" and 'Gearheart_P-Styles_AEquationCenter' in child.get('class',[]):
                print("equation from Gearheart_P-Styles_AEquationCenter tag")
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

            # handling equaiton as text
            elif child.name == "math":
                print("equation here")
                equation_Id = generate_unique_id()
                eqaution_data = {"id": equation_Id, "math_tag": str(child)}
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
                print("hello")
                toc = get_toc_from_ncx(toc_content)
            else:
                print("hello2")
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



s3 = []
not_s3 = []
for book in publisher_collection.find():
    if (
        "publishers" in book
        and book["publishers"]
        and book["publishers"][0].startswith("SAS")
    ):
        if "s3_key" in book:
            s3_key = book["s3_key"]
            bookname = book["s3_key"].split("/")[-2]
            already_extracted = extracted_books.find_one({"book": bookname})
            if not already_extracted:
                print("bookname", bookname)
                get_book_data(bookname)
            else:
                print(f"this {bookname}already extracted")
            s3.append(bookname)
        else:
            not_s3.append(book)

print("Total s3", len(s3))
print("Total not s3", len(not_s3))

# get_book_data('End-to-End Data Science with SAS (9781642958065)')
